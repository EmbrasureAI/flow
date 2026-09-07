"""Collect diagnostics and remove only a container owned by the calling fixture."""
import subprocess


def cleanup_container(name, log_path=None):
    result = {"container": name, "passed": True}
    try:
        if log_path is not None:
            logs = subprocess.run(["docker", "logs", name], capture_output=True, timeout=30)
            log_path.write_bytes(logs.stdout + logs.stderr)
            if logs.returncode:
                result.update(passed=False, collection_error=f"docker logs exited {logs.returncode}")
    except Exception as error:
        result.update(passed=False, collection_error=f"{type(error).__name__}: {error}")
    finally:
        try:
            removed = subprocess.run(["docker", "rm", "--force", "--volumes", name],
                                     capture_output=True, text=True, timeout=30)
            absent = "No such container" in removed.stderr
            result.update(returncode=removed.returncode, absent=absent, stderr=removed.stderr)
            if removed.returncode and not absent:
                result["passed"] = False
        except Exception as error:
            result.update(passed=False, cleanup_error=f"{type(error).__name__}: {error}")
    return result
