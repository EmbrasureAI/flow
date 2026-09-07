#!/usr/bin/env python3
"""Validate a fresh Docker demo, retain its evidence, and remove only its own project."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time
import traceback


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


class Validation:
    def __init__(self, args):
        self.args = args
        self.directory = args.artifacts.resolve()
        self.directory.mkdir(parents=True, exist_ok=False)
        self.compose = ["docker", "compose", "-p", args.project, "-f", str(Path(__file__).with_name("compose.yaml").resolve())]
        self.report = {"project": args.project, "phases": [], "passed": False}
        self.created = False

    def run(self, arguments, check=True, timeout=120):
        result = subprocess.run(arguments, capture_output=True, text=True, timeout=timeout)
        if check and result.returncode:
            raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(arguments)}\n{result.stdout}\n{result.stderr}")
        return result

    def phase(self, name, action):
        print(f"[{name}]", flush=True)
        start = time.monotonic()
        details = action()
        self.report["phases"].append({"name": name, "seconds": round(time.monotonic() - start, 3), "details": details})
        write_json(self.directory / "report.json", self.report)

    def until(self, description, condition):
        deadline = time.monotonic() + self.args.timeout
        while time.monotonic() < deadline:
            result = condition()
            if result:
                return result
            time.sleep(0.25)
        raise TimeoutError(description)

    def start(self):
        label = f"label=com.docker.compose.project={self.args.project}"
        for kind, arguments in (("containers", ["ps", "-a", "-q"]), ("volumes", ["volume", "ls", "-q"]), ("networks", ["network", "ls", "-q"])):
            existing = self.run(["docker", *arguments, "--filter", label]).stdout.strip()
            if existing:
                raise RuntimeError(f"Refusing to reuse existing project {self.args.project} {kind}: {existing}")
        config = json.loads(self.run([*self.compose, "config", "--format", "json"]).stdout)
        assert all(not service.get("ports") for service in config["services"].values()), "demo must not publish host ports"
        if not self.args.skip_build:
            with (self.directory / "build.log").open("w") as log:
                subprocess.run([*self.compose, "build"], stdout=log, stderr=subprocess.STDOUT, check=True, timeout=3600)
        self.created = True
        started = self.run([*self.compose, "up", "-d", "--no-build"], timeout=self.args.timeout)
        (self.directory / "up.log").write_text(started.stdout + started.stderr)
        self.until("flow never became ready", lambda: self.status(check=False).get("ready"))
        self.until("Trino never accepted a query", lambda: self.trino("SELECT 1 AS ready", check=False).returncode == 0)
        return {"host_ports": [], "image": self.run([*self.compose, "images", "--format", "json"]).stdout}

    def status(self, check=True, stopped=False):
        command = ([*self.compose, "run", "--rm", "--no-deps", "flow"] if stopped
                   else [*self.compose, "exec", "-T", "flow", "embrasure-flow"])
        result = self.run([*command, "--config", "/etc/flow.toml", "status"], check=check)
        return json.loads(result.stdout) if result.returncode == 0 else {}

    def psql(self, query):
        return self.run([*self.compose, "exec", "-T", "postgres", "psql", "-U", "flow", "-d", "flow", "-v", "ON_ERROR_STOP=1", "-qAt", "-c", query]).stdout.strip()

    def trino(self, query, check=True):
        return self.run([*self.compose, "exec", "-T", "trino", "trino", "--user", "demo", "--output-format", "JSON", "--execute", query], check=check)

    def verify(self, phase):
        expected = json.loads(self.psql("SELECT json_agg(t) FROM (SELECT id, status FROM orders ORDER BY id) t"))
        def matches():
            result = self.trino("SELECT id, status FROM lake.replicated.orders ORDER BY id", check=False)
            if result.returncode:
                return None
            actual = [json.loads(line) for line in result.stdout.splitlines() if line]
            return actual if actual == expected else None
        actual = self.until(f"{phase}: Trino rows did not match PostgreSQL", matches)
        write_json(self.directory / f"{phase}-rows.json", actual)
        status = self.status()
        assert status["ready"]
        write_json(self.directory / f"{phase}-status.json", status)
        return {"rows": actual, "status": status}

    def permissions(self):
        identity = self.run([*self.compose, "exec", "-T", "flow", "id", "-u"]).stdout.strip()
        assert identity == "10001", f"daemon is running as unexpected UID: {identity}"
        ownership = self.run([*self.compose, "exec", "-T", "flow", "stat", "-c", "%u:%g %n", "/data", "/data/control", "/data/index"]).stdout
        assert all(line.startswith("10001:10001 ") for line in ownership.splitlines()), ownership
        binary = self.run([*self.compose, "exec", "-T", "flow", "sha256sum", "/usr/local/bin/embrasure-flow"]).stdout.strip()
        return {"uid": identity, "ownership": ownership, "binary": binary}

    def mutations(self):
        self.psql("""BEGIN;
INSERT INTO orders VALUES (2, 'paid'), (3, 'cancelled'), (4, NULL);
UPDATE orders SET status='shipped' WHERE id=1;
DELETE FROM orders WHERE id=3;
UPDATE orders SET id=20 WHERE id=2;
COMMIT;""")
        return self.verify("crud")

    def terminate(self):
        before = self.status()
        started = time.monotonic()
        self.run([*self.compose, "stop", "--timeout", "30", "flow"])
        container = self.run([*self.compose, "ps", "-a", "-q", "flow"]).stdout.strip()
        state = json.loads(self.run(["docker", "inspect", "--format", "{{json .State}}", container]).stdout)
        assert state["ExitCode"] == 0 and not state["OOMKilled"], state
        status = self.status(stopped=True)
        assert not status["ready"], status
        assert status["watermarks"] == before["watermarks"], "graceful stop changed durable progress observation"
        write_json(self.directory / "stopped-status.json", status)
        return {"seconds": round(time.monotonic() - started, 3), "container": state, "status": status}

    def restart(self):
        self.psql("UPDATE orders SET status='delivered' WHERE id=1; DELETE FROM orders WHERE id=4; INSERT INTO orders VALUES (5, 'queued-during-stop')")
        self.run([*self.compose, "start", "flow"])
        result = self.verify("restart")
        # The initializer is idempotent against its durable bootstrap and does
        # not reset the permanent source slot or overwrite a live base.
        slot = self.psql("SELECT restart_lsn::text FROM pg_replication_slots WHERE slot_name='embrasure_flow'")
        self.run([*self.compose, "stop", "--timeout", "30", "flow"])
        self.run([*self.compose, "run", "--rm", "--no-deps", "initialize"])
        assert self.psql("SELECT restart_lsn::text FROM pg_replication_slots WHERE slot_name='embrasure_flow'") == slot
        self.run([*self.compose, "start", "flow"])
        result["idempotent_initialize"] = self.verify("reinitialize")
        return result

    def execute(self):
        try:
            self.phase("fresh-stack", self.start)
            self.phase("nonroot-volume-permissions", self.permissions)
            self.phase("initial-independent-reader", lambda: self.verify("initial"))
            self.phase("insert-update-delete-keymove", self.mutations)
            self.phase("sigterm-status", self.terminate)
            self.phase("restart-with-source-backlog", self.restart)
            self.report["passed"] = True
        except BaseException as error:
            self.report["failure"] = str(error)
            self.report["traceback"] = traceback.format_exc()
            raise
        finally:
            failure_in_flight = sys.exc_info()[0] is not None
            if self.created:
                try:
                    logs = self.run([*self.compose, "logs", "--no-color"], check=False)
                    (self.directory / "compose.log").write_text(logs.stdout + logs.stderr)
                    (self.directory / "containers.json").write_text(self.run([*self.compose, "ps", "-a", "--format", "json"], check=False).stdout)
                except Exception as error:
                    self.report["collection_error"] = str(error)
                    self.report["passed"] = False
                finally:
                    try:
                        cleanup = self.run([*self.compose, "down", "--volumes", "--remove-orphans"], check=False)
                        self.report["cleanup_exit_code"] = cleanup.returncode
                        if cleanup.returncode:
                            self.report["passed"] = False
                        (self.directory / "cleanup.log").write_text(cleanup.stdout + cleanup.stderr)
                    except Exception as error:
                        self.report["cleanup_error"] = str(error)
                        self.report["passed"] = False
            try:
                write_json(self.directory / "report.json", self.report)
            except OSError as error:
                if not failure_in_flight:
                    raise
                print(f"Could not save failure report: {error}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="flow-demo-validation")
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=240)
    parser.add_argument("--skip-build", action="store_true", help="use images built from the current checkout")
    args = parser.parse_args()
    validation = Validation(args)
    validation.execute()
    if not validation.report["passed"]:
        raise SystemExit(1)
    print(f"PASS: {args.artifacts.resolve() / 'report.json'}")


if __name__ == "__main__":
    main()
