#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5"]
# ///
"""Open-loop CDC freshness, recovery and exact-row verification for remote durability.

Runs one Flow process against PostgreSQL and an Iceberg REST catalog, with or
without `[durability]`, using the production fixture stack (MinIO and the
Iceberg REST fixture) on loopback ports. Catalog requests pass through a local
fault proxy; an optional proxy adds S3-like latency to Flow's object requests.

Every successful run requires every committed transaction marker in every table
and an exact sorted comparison of ``(id, version, md5(payload))`` against
PostgreSQL. Failures are reported, never retried into success.
"""
import argparse
import hashlib
import http.client
import http.server
import json
import os
from pathlib import Path
import random
import signal
import socket
import subprocess
import threading
import time
import traceback
import urllib.error
import urllib.request
from uuid import uuid4

import duckdb
import psycopg
from psycopg import sql

def percentile(values):
    values = sorted(values)
    if not values:
        return {'n': 0}
    pick = lambda p: values[min(len(values) - 1, int(p * (len(values) - 1)))]
    return {'n': len(values), 'p50': pick(.5), 'p90': pick(.9), 'p95': pick(.95),
            'p99': pick(.99), 'max': values[-1], 'mean': sum(values) / len(values)}


def lsn(value):
    if isinstance(value, int):
        return value
    if '/' in str(value):
        high, low = str(value).split('/')
        return (int(high, 16) << 32) | int(low, 16)
    return int(value)


class CatalogFaultProxy:
    """Loopback HTTP proxy in front of the REST fixture with fault injection.

    Table POSTs fail with 503 until `blocked_until`; `drop_next_commit`
    replaces the next successful table POST with a 502. Every forwarded
    request is recorded in `report['catalog_http']`."""
    def __init__(self, upstream, report):
        self.upstream = upstream.rstrip('/')
        self.report = report
        self.blocked_until = 0.0
        self.drop_next_commit = False
        self.lock = threading.Lock()

    def before(self, method, path):
        commit = method == 'POST' and '/tables/' in path
        if commit and time.monotonic() < self.blocked_until:
            return 503, b'{"error":{"message":"injected outage","type":"ServiceUnavailableException","code":503}}'
        return None

    def after(self, method, path, status, started):
        commit = method == 'POST' and '/tables/' in path
        with self.lock:
            self.report.setdefault('catalog_http', []).append({
                'method': method, 'kind': 'table' if '/tables/' in path else 'other',
                'ms': (time.monotonic() - started) * 1000, 'status': status})
            if commit and self.drop_next_commit and 200 <= status < 300:
                self.drop_next_commit = False
                self.report.setdefault('lost_responses', []).append({'at_ms': time.time_ns() / 1e6})
                return 502, b'{"error":{"message":"injected ambiguous outcome","type":"ServiceUnavailableException","code":502}}'
        return None

    def __enter__(self):
        proxy = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def log_message(self, *_):
                pass

            def handle_any(self):
                length = int(self.headers.get('Content-Length') or 0)
                body = self.rfile.read(length) if length else None
                started = time.monotonic()
                injected = proxy.before(self.command, self.path)
                if injected is None:
                    request = urllib.request.Request(proxy.upstream + self.path, data=body, method=self.command)
                    for name in ('Content-Type', 'Authorization'):
                        if self.headers.get(name):
                            request.add_header(name, self.headers[name])
                    try:
                        with urllib.request.urlopen(request, timeout=60) as response:
                            status, payload = response.status, response.read()
                    except urllib.error.HTTPError as error:
                        status, payload = error.code, error.read()
                    replaced = proxy.after(self.command, self.path, status, started)
                    if replaced is not None:
                        status, payload = replaced
                else:
                    status, payload = injected
                self.send_response(status)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            do_GET = do_POST = do_HEAD = do_DELETE = handle_any

        self.server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        return f'http://127.0.0.1:{self.server.server_address[1]}'

    def __exit__(self, *_):
        self.server.shutdown()


class S3LatencyProxy:
    """Loopback S3 proxy adding object-store latency sampled from measured
    S3 Standard requests in us-east-1 (p50 32 ms, p90 43 ms, p99 ~105 ms).
    The Host header is preserved so SigV4 signatures remain valid."""
    def __init__(self, upstream, median_ms, rng, log_path=None):
        self.upstream = upstream.split('://', 1)[1]
        self.median = median_ms / 1000
        self.rng = rng
        self.lock = threading.Lock()
        self.log = log_path.open('w') if log_path else None

    def delay(self, size):
        with self.lock:
            tail = self.rng.random() < 0.03
            base = self.rng.uniform(0.08, 0.15) if tail else self.median * self.rng.lognormvariate(0, 0.3)
        return base + size / (120 * 2**20)

    def __enter__(self):
        proxy = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def log_message(self, *_):
                pass

            def handle_any(self):
                length = int(self.headers.get('Content-Length') or 0)
                body = self.rfile.read(length) if length else None
                started = time.time()
                time.sleep(proxy.delay(length))
                upstream = http.client.HTTPConnection(proxy.upstream, timeout=120)
                upstream.putrequest(self.command, self.path, skip_host=True, skip_accept_encoding=True)
                for key, value in self.headers.items():
                    upstream.putheader(key, value)
                upstream.endheaders()
                if body:
                    upstream.send(body)
                response = upstream.getresponse()
                payload = response.read()
                self.send_response(response.status)
                head = self.command == 'HEAD'
                for key, value in response.getheaders():
                    if key.lower() in ('transfer-encoding', 'connection'):
                        continue
                    if key.lower() == 'content-length' and not head:
                        continue
                    self.send_header(key, value)
                if not head:
                    self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                if self.command != 'HEAD':
                    self.wfile.write(payload)
                upstream.close()
                if proxy.log:
                    with proxy.lock:
                        proxy.log.write(json.dumps({'t': started, 'ms': (time.time() - started) * 1000,
                                                    'm': self.command, 'p': self.path[:160], 'in': length,
                                                    'out': len(payload), 's': response.status}) + '\n')
                        proxy.log.flush()

            do_GET = do_PUT = do_POST = do_HEAD = do_DELETE = handle_any

        self.server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server.daemon_threads = True
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        return f'http://127.0.0.1:{self.server.server_address[1]}'

    def __exit__(self, *_):
        self.server.shutdown()
        if self.log:
            self.log.close()


def free_port_pair():
    """Two adjacent free loopback ports; a stale-owner replacement listens on the second."""
    while True:
        with socket.socket() as first, socket.socket() as second:
            first.bind(('127.0.0.1', 0))
            port = first.getsockname()[1]
            try:
                second.bind(('127.0.0.1', port + 1))
            except (OSError, OverflowError):
                continue
            return port, port + 1


def metadata_location(args, table):
    url = f'{args.rest_uri.rstrip("/")}/v1/namespaces/{args.namespace}/tables/{table}'
    with urllib.request.urlopen(url, timeout=30) as response:
        return json.load(response)['metadata-location']


def open_reader(args):
    config = {'memory_limit': args.reader_memory, 'threads': 2}
    if args.duckdb_extensions:
        config['extension_directory'] = args.duckdb_extensions
    connection = duckdb.connect(config=config)
    for extension in ('httpfs', 'iceberg'):
        connection.execute(f'INSTALL {extension}')
        connection.execute(f'LOAD {extension}')
    endpoint = args.s3_endpoint.split('://', 1)[1]
    connection.execute(f"CREATE SECRET local (TYPE S3, KEY_ID 'demo-access', SECRET 'demo-secret-key', "
                       f"ENDPOINT '{endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')")
    return connection


def log_events(logs):
    """Structured `fields` of every JSON line in Flow's logs."""
    for path in logs:
        for line in path.read_text().splitlines():
            try:
                yield json.loads(line).get('fields', {})
            except ValueError:
                continue


def scrape(port):
    try:
        with urllib.request.urlopen(f'http://127.0.0.1:{port}/metrics', timeout=5) as response:
            return response.read().decode()
    except Exception:
        return None


def histogram(metrics_text, name, labels=None):
    """Sum and count of a Prometheus summary/histogram family, if present."""
    total = count = 0.0
    if not metrics_text:
        return None
    for line in metrics_text.splitlines():
        if line.startswith('#'):
            continue
        if labels and not all(f'{k}="{v}"' in line for k, v in labels.items()):
            continue
        if line.startswith(name + '_sum'):
            total += float(line.rsplit(' ', 1)[1])
        elif line.startswith(name + '_count'):
            count += float(line.rsplit(' ', 1)[1])
    return {'sum': total, 'count': count, 'mean': total / count if count else None}


def quantiles(metrics_text, name, labels=None):
    out = {}
    if not metrics_text:
        return out
    for line in metrics_text.splitlines():
        if not line.startswith(name + '{') or 'quantile=' not in line:
            continue
        if labels and not all(f'{k}="{v}"' in line for k, v in labels.items()):
            continue
        quantile = line.split('quantile="', 1)[1].split('"', 1)[0]
        out[quantile] = float(line.rsplit(' ', 1)[1])
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--label', required=True)
    parser.add_argument('--root', type=Path, required=True, help='new run directories are created here')
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--pg', default='host=127.0.0.1 port=55434 dbname=fixture user=flow password=flow')
    parser.add_argument('--rest-uri', default='http://127.0.0.1:8181')
    parser.add_argument('--s3-endpoint', default='http://127.0.0.1:9000')
    parser.add_argument('--namespace', default='bench')
    parser.add_argument('--durability', choices=['none', 'remote'], default='none')
    parser.add_argument('--durability-uri', help='override the replicated prefix (a run directory is appended)')
    parser.add_argument('--durability-toml', default='', help='extra [durability] settings, TOML lines')
    parser.add_argument('--ack-mode', choices=['materialized', 'journaled'], default='materialized')
    parser.add_argument('--tables', type=int, default=1)
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--seconds', type=float, default=60)
    parser.add_argument('--tps', type=float, default=5)
    parser.add_argument('--mutations', type=int, default=10)
    parser.add_argument('--seed-rows', type=int, default=10000)
    parser.add_argument('--format-version', type=int, choices=[2, 3], default=2)
    parser.add_argument('--maintenance', choices=['current', 'disabled'], default='current')
    parser.add_argument('--adversarial', action='store_true', help='net deletes and rolled-back transactions')
    parser.add_argument('--write-during-copy', action='store_true')
    parser.add_argument('--fault', default='none',
                        choices=['none', 'crash', 'disk-loss', 'disk-loss-backlog', 'catalog-outage',
                                 'lost-catalog-response', 'stale-owner', 'clean-restart'])
    parser.add_argument('--limits-toml', default='', help='extra [limits] settings, TOML lines')
    parser.add_argument('--settle-seconds', type=float, default=240)
    parser.add_argument('--reader-interval', type=float, default=0.25)
    parser.add_argument('--duckdb-extensions', help='DuckDB extension directory (offline hosts)')
    parser.add_argument('--reader-memory', default='1GB')
    parser.add_argument('--s3-latency-ms', type=float, default=0,
                        help='median latency added to Flow\'s object-store requests')
    args = parser.parse_args()
    assert args.fault != 'disk-loss-backlog' or (args.durability == 'remote' and args.ack_mode == 'journaled')
    assert args.fault not in ('disk-loss', 'stale-owner') or args.durability == 'remote'
    assert args.ack_mode != 'journaled' or args.durability == 'remote'

    name = 'dur_' + uuid4().hex[:10]
    directory = args.root / args.label
    directory.mkdir(parents=True, exist_ok=False)
    tables = [f'{name}_{i}' for i in range(args.tables)]
    report = {'run': name, 'label': args.label, 'parameters': {k: str(v) for k, v in vars(args).items()},
              'passed': False, 'transactions': [], 'checks': [], 'reader_failures': [], 'events': [],
              'binary_sha256': hashlib.file_digest(args.binary.open('rb'), 'sha256').hexdigest(),
              'harness_sha256': hashlib.file_digest(Path(__file__).open('rb'), 'sha256').hexdigest()}
    catalog_proxy = CatalogFaultProxy(args.rest_uri, report)
    flow_s3_endpoint = args.s3_endpoint
    latency_proxy = None
    if args.s3_latency_ms > 0:
        latency_proxy = S3LatencyProxy(args.s3_endpoint, args.s3_latency_ms, random.Random(7),
                                       directory / 's3-requests.jsonl')
        flow_s3_endpoint = latency_proxy.__enter__()
    http_port, replacement_port = free_port_pair()
    processes = []
    logs = []
    stopped = threading.Event()
    lock = threading.Lock()
    metrics_samples = []
    state = directory / 'state'

    def save():
        (directory / 'report.json').write_text(json.dumps(report, indent=2, default=str))

    def event(kind, **fields):
        report['events'].append({'kind': kind, 'at_ms': time.time_ns() / 1e6, **fields})

    def start(env, config, state_dir=None, label=None):
        path = directory / f'flow-{len(logs)}.log'
        logs.append(path)
        roles = ['--roles', 'ingest,coordinator'] if args.maintenance == 'disabled' else []
        config_path = config if state_dir is None else rewrite(config, state_dir, label)
        process = subprocess.Popen([str(args.binary), '--config', str(config_path), 'run', *roles],
                                   env=env, stdout=path.open('w'), stderr=subprocess.STDOUT)
        processes.append(process)
        event('start', pid=process.pid, log=path.name)
        return process

    def rewrite(config, state_dir, label):
        text = config.read_text().replace(json.dumps(str(state)), json.dumps(str(state_dir)))
        text = text.replace(f'listen = "127.0.0.1:{http_port}"', f'listen = "127.0.0.1:{replacement_port}"')
        path = directory / f'flow-{label}.toml'
        path.write_text(text)
        return path

    def stop(process, crash=False, timeout=60):
        if process is None or process.poll() is not None:
            return process.returncode if process else None
        process.send_signal(signal.SIGKILL if crash else signal.SIGTERM)
        try:
            process.wait(timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(10)
        event('stop', pid=process.pid, crash=crash, returncode=process.returncode)
        return process.returncode

    def observe():
        try:
            reader = open_reader(args)
            while not stopped.is_set():
                for index, table in enumerate(tables):
                    try:
                        location = metadata_location(args, table)
                        seen = {int(-row[0]) for row in reader.execute(
                            'SELECT id FROM iceberg_scan(?) WHERE id < 0', [location]).fetchall()}
                        now = time.time_ns() / 1e6
                        with lock:
                            for txn in report['transactions']:
                                if txn['sequence'] in seen:
                                    txn.setdefault('reader_ms', {}).setdefault(str(index), now)
                    except Exception as error:
                        report['reader_failures'].append({'at': time.time(), 'error': str(error)[:300]})
                stopped.wait(args.reader_interval)
        except Exception as error:
            report['reader_fatal'] = str(error)

    def sample():
        while not stopped.wait(2):
            current = next((p for p in reversed(processes) if p.poll() is None), None)
            if current is None:
                continue
            try:
                stat = Path(f'/proc/{current.pid}/stat').read_text().split()
                metrics_samples.append({'at_ms': time.time_ns() / 1e6, 'pid': current.pid,
                    'cpu_seconds': (int(stat[13]) + int(stat[14])) / os.sysconf('SC_CLK_TCK'),
                    'rss_mib': int(stat[23]) * os.sysconf('SC_PAGE_SIZE') / 2**20})
            except (OSError, IndexError):
                pass

    sampler = threading.Thread(target=sample, daemon=True)
    sampler.start()
    pg = None
    try:
        pg = psycopg.connect(args.pg, autocommit=True)
        report['postgres'] = {k: pg.execute(f'SHOW {k}').fetchone()[0]
                              for k in ('server_version', 'fsync', 'synchronous_commit', 'wal_level')}
        pg.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(name)))
        for table in tables:
            ident = sql.Identifier(name, table)
            pg.execute(sql.SQL('CREATE TABLE {} (id bigint PRIMARY KEY, version bigint NOT NULL, payload text NOT NULL)').format(ident))
            pg.execute(sql.SQL("INSERT INTO {} SELECT i, 0, repeat(md5(i::text), 8) FROM generate_series(1, %s) i").format(ident), [args.seed_rows])
            pg.execute(sql.SQL('ALTER TABLE {} REPLICA IDENTITY FULL').format(ident))
        pg.execute(sql.SQL('CREATE PUBLICATION {} FOR TABLE {}').format(
            sql.Identifier(name), sql.SQL(',').join(sql.Identifier(name, t) for t in tables)))
        request = urllib.request.Request(f'{args.rest_uri.rstrip("/")}/v1/namespaces', method='POST',
                                         data=json.dumps({'namespace': [args.namespace]}).encode(),
                                         headers={'Content-Type': 'application/json'})
        try:
            urllib.request.urlopen(request, timeout=30).read()
        except urllib.error.HTTPError as error:
            if error.code != 409:
                raise
        with catalog_proxy as catalog_uri:
            text = f'state_dir = {json.dumps(str(state))}\n'
            text += f'[source]\nconnection_env = "FLOW_BENCH_PG"\nid = "{name}"\nslot = "{name}"\n'
            text += f'publication = "{name}"\nack_mode = "{args.ack_mode}"\n'
            text += (f'[catalog]\nuri = {json.dumps(catalog_uri)}\nwarehouse = "s3://warehouse/"\n'
                     f'"s3.endpoint" = {json.dumps(flow_s3_endpoint)}\n"s3.region" = "us-east-1"\n'
                     '"s3.path-style-access" = "true"\n')
            text += (f'[limits]\ntable_workers = {args.workers}\ncommits_per_second = 100\n'
                     'journal_bytes = 4294967296\nspool_bytes = 2147483648\n' + args.limits_toml + '\n')
            text += f'[storage]\nmin_free_bytes = 0\n[http]\nlisten = "127.0.0.1:{http_port}"\n'
            if args.maintenance == 'disabled':
                text += ('[compaction]\nl0_soft_files = 1000000\nl0_hard_files = 2000000\n'
                         'oldest_l0_soft_ms = 100000000\noldest_l0_hard_ms = 200000000\n'
                         'delete_files_soft = 1000000\ndelete_files_hard = 2000000\n')
            if args.durability == 'remote':
                durability_uri = (args.durability_uri.rstrip('/') + '/' + name if args.durability_uri
                                  else f's3://durability/{name}')
                text += f'[durability]\nuri = {json.dumps(durability_uri)}\n'
                text += f'endpoint = {json.dumps(flow_s3_endpoint)}\n'
                text += args.durability_toml + '\n'
            for table in tables:
                text += (f'[[tables]]\nsource_namespace = "{name}"\nsource_table = "{table}"\n'
                         f'target_namespace = ["{args.namespace}"]\ntarget_table = "{table}"\n'
                         f'primary_key = [0]\nformat_version = {args.format_version}\ncolumns = [\n'
                         ' {field_id=1,name="id",data_type="Int64",nullable=false},\n'
                         ' {field_id=2,name="version",data_type="Int64",nullable=false},\n'
                         ' {field_id=3,name="payload",data_type="String",nullable=false}\n]\n')
            config = directory / 'flow.toml'
            config.write_text(text)
            config.chmod(0o600)
            env = dict(os.environ) | {'AWS_ACCESS_KEY_ID': 'demo-access', 'AWS_SECRET_ACCESS_KEY': 'demo-secret-key',
                                      'AWS_REGION': 'us-east-1', 'FLOW_BENCH_PG': args.pg + ' sslmode=disable',
                                      'RUST_LOG': 'info,flow_events=debug'}

            copying = threading.Event()

            def mutate_during_copy():
                with psycopg.connect(args.pg, autocommit=True) as writer:
                    while not copying.is_set():
                        if writer.execute('SELECT 1 FROM pg_replication_slots WHERE slot_name=%s', [name]).fetchone():
                            break
                        copying.wait(.05)
                    for sequence in range(1, 101):
                        if copying.is_set():
                            break
                        with writer.transaction():
                            for table in tables:
                                ident = sql.Identifier(name, table)
                                writer.execute(sql.SQL('UPDATE {} SET payload=%s WHERE id=%s').format(ident), ['during-copy-' + str(sequence), sequence])
                                writer.execute(sql.SQL('INSERT INTO {} VALUES(%s,%s,%s)').format(ident), [args.seed_rows + sequence, sequence, 'during-copy-insert'])
                        report['copy_writer_commits'] = sequence
                        copying.wait(.05)

            copy_writer = threading.Thread(target=mutate_during_copy, daemon=True) if args.write_during_copy else None
            if copy_writer:
                copy_writer.start()
            began = time.monotonic()
            try:
                with (directory / 'init.log').open('w') as log:
                    subprocess.run([str(args.binary), '--config', str(config), 'init'], env=env,
                                   stdout=log, stderr=subprocess.STDOUT, check=True, timeout=1800)
            finally:
                copying.set()
                if copy_writer:
                    copy_writer.join(30)
            report['initial_copy_seconds'] = time.monotonic() - began
            process = start(env, config)
            observer = threading.Thread(target=observe, daemon=True)
            observer.start()
            began = time.monotonic()
            failed = False
            rng = random.Random(42)
            stale = None
            total = int(args.seconds * args.tps)
            with psycopg.connect(args.pg) as writer:
                for sequence in range(1, total + 1):
                    scheduled = began + (sequence - 1) / args.tps
                    time.sleep(max(0, scheduled - time.monotonic()))
                    if process.poll() is not None and args.fault != 'stale-owner':
                        raise RuntimeError(f'Flow exited {process.returncode}')
                    arrived = time.monotonic()
                    keys = [rng.randint(1, args.seed_rows) for _ in range(args.mutations)]
                    xid = int(writer.execute('SELECT pg_current_xact_id()::text').fetchone()[0]) & 0xffffffff
                    for table in tables:
                        ident = sql.Identifier(name, table)
                        writer.execute(sql.SQL('UPDATE {} SET version=%s WHERE id=ANY(%s)').format(ident), [sequence, keys])
                        writer.execute(sql.SQL('DELETE FROM {} WHERE id=%s').format(ident), [keys[0]])
                        writer.execute(sql.SQL('INSERT INTO {} VALUES(%s,%s,%s)').format(ident), [keys[0], sequence, 'reinsert-' + str(sequence)])
                        writer.execute(sql.SQL('INSERT INTO {} VALUES(%s,%s,%s)').format(ident), [-sequence, sequence, 'marker-' + str(sequence)])
                        if args.adversarial and sequence % 7 == 0:
                            writer.execute(sql.SQL('DELETE FROM {} WHERE id=%s').format(ident), [sequence])
                    writer.commit()
                    with lock:
                        report['transactions'].append({'sequence': sequence, 'xid': xid, 'response_ms': time.time_ns() / 1e6,
                                                       'arrival_delay_ms': (arrived - scheduled) * 1000})
                    if args.adversarial and sequence % 11 == 0:
                        for table in tables:
                            ident = sql.Identifier(name, table)
                            writer.execute(sql.SQL("INSERT INTO {} VALUES(%s,-1,'must-roll-back')").format(ident), [-1000000 - sequence])
                            writer.execute(sql.SQL('UPDATE {} SET version=-1 WHERE id=1').format(ident))
                        writer.rollback()
                    third = max(1, total // 3)
                    if args.fault in ('disk-loss-backlog', 'catalog-outage') and sequence == third:
                        catalog_proxy.blocked_until = time.monotonic() + 15
                        event('catalog-outage', seconds=15)
                    if args.fault == 'lost-catalog-response' and sequence == third:
                        catalog_proxy.drop_next_commit = True
                        event('drop-next-commit')
                    if args.fault == 'stale-owner' and sequence == third and stale is None:
                        # A replacement starts on a fresh disk while the old owner runs.
                        stale = process
                        process = start(env, config, state_dir=directory / 'replacement-state', label='replacement')
                        report['stale_owner'] = {'old_pid': stale.pid, 'new_pid': process.pid}
                    if args.fault in ('crash', 'disk-loss', 'disk-loss-backlog', 'clean-restart') and not failed and sequence >= total // 2:
                        failed = True
                        fault_wall = time.time_ns() / 1e6
                        fault_started = time.monotonic()
                        stop(process, crash=args.fault != 'clean-restart')
                        if args.fault == 'disk-loss-backlog':
                            slot = pg.execute('SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=%s', [name]).fetchone()[0]
                            report['pre_loss_slot'] = slot
                            reader = open_reader(args)
                            visible = [{int(-row[0]) for row in reader.execute(
                                'SELECT id FROM iceberg_scan(?) WHERE id < 0', [metadata_location(args, t)]).fetchall()}
                                for t in tables]
                            journaled = {fields['xid']: lsn(fields['end_lsn']) for fields in log_events(logs)
                                         if fields.get('event') == 'transaction_journaled'}
                            missing = [t['sequence'] for t in report['transactions']
                                       if t['xid'] in journaled and journaled[t['xid']] <= lsn(slot)
                                       and any(t['sequence'] not in seen for seen in visible)]
                            report['acknowledged_missing_before_loss'] = missing
                            assert missing, 'fault did not cover acknowledged, unpublished transactions'
                        if args.fault in ('disk-loss', 'disk-loss-backlog'):
                            state.rename(directory / 'lost-state')
                        process = start(env, config)
                        report['fault'] = {'kind': args.fault, 'after_sequence': sequence, 'at_ms': fault_wall,
                                           'restart_after_seconds': time.monotonic() - fault_started}
            report['write_window_seconds'] = time.monotonic() - began
            if stale is not None:
                deadline = time.monotonic() + 60
                while stale.poll() is None and time.monotonic() < deadline:
                    time.sleep(.2)
                report['stale_owner']['old_exit'] = stale.poll()
                assert stale.poll() is not None, 'replaced owner kept running'
                old_log = logs[0].read_text()
                report['stale_owner']['fenced'] = 'newer owner' in old_log
            deadline = time.monotonic() + args.settle_seconds
            complete = False
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError(f'Flow exited {process.returncode}')
                with lock:
                    complete = all(len(t.get('reader_ms', {})) == args.tables for t in report['transactions'])
                if complete:
                    break
                time.sleep(.5)
            report['all_markers_observed'] = complete
            assert complete, 'timed out waiting for every marker in every Iceberg table'
            metrics_text = scrape(replacement_port if stale is not None else http_port)
            stopped.set()
            observer.join(30)
            reader = open_reader(args)
            normalize = lambda rows: [tuple(v.decode() if isinstance(v, bytes) else v for v in row) for row in rows]
            for table in tables:
                source = pg.execute(sql.SQL('SELECT id, version, md5(payload) FROM {} ORDER BY id').format(sql.Identifier(name, table))).fetchall()
                target = reader.execute('SELECT id, version, md5(payload) FROM iceberg_scan(?) ORDER BY id',
                                        [metadata_location(args, table)]).fetchall()
                source, target = normalize(source), normalize(target)
                assert source == target, f'exact row mismatch: {table}'
                report['checks'].append({'table': table, 'exact_rows': len(source),
                                         'digest': hashlib.sha256(repr(source).encode()).hexdigest()})
            report['slot'] = pg.execute('SELECT confirmed_flush_lsn::text, restart_lsn::text FROM pg_replication_slots WHERE slot_name=%s', [name]).fetchone()
            if metrics_text:
                (directory / 'metrics.prom').write_text(metrics_text)
                report['durability_metrics'] = {
                    'barrier_seconds': histogram(metrics_text, 'flow_durability_barrier_seconds'),
                    'barrier_quantiles': quantiles(metrics_text, 'flow_durability_barrier_seconds'),
                    'log_put_seconds': histogram(metrics_text, 'flow_durability_put_seconds', {'kind': 'log'}),
                    'log_put_quantiles': quantiles(metrics_text, 'flow_durability_put_seconds', {'kind': 'log'}),
                    'batch_records': histogram(metrics_text, 'flow_durability_batch_records'),
                    'batch_bytes': histogram(metrics_text, 'flow_durability_batch_bytes'),
                    'checkpoint_seconds': histogram(metrics_text, 'flow_durability_checkpoint_seconds'),
                }
            status_path = (directory / 'replacement-state' if stale is not None else state) / 'status.json'
            if status_path.exists():
                report['final_status_durability'] = json.loads(status_path.read_text()).get('durability')
            stop(process)
        journaled, published = {}, []
        for fields in log_events(logs):
            if fields.get('event') == 'transaction_journaled':
                journaled[fields['xid']] = fields
            elif fields.get('event') == 'table_published' and not fields.get('already_committed'):
                published.append(fields)
            elif fields.get('event') == 'durability_recovered':
                report.setdefault('recoveries', []).append(fields)
        catalog, reader_latency = [], []
        for txn in report['transactions']:
            j = journaled.get(txn['xid'])
            if j:
                matches = [p for p in published if lsn(p['first_lsn']) <= lsn(j['end_lsn']) <= lsn(p['last_lsn'])]
                by_table = {p['table_id']: p for p in matches}
                if len(by_table) == args.tables:
                    committed = max(p['catalog_committed_at_micros'] for p in by_table.values()) / 1000
                    catalog.append(committed - j['commit_timestamp_micros'] / 1000)
            reader_latency.append(max(txn['reader_ms'].values()) - txn['response_ms'])
        workload = {t['xid'] for t in report['transactions']}
        report['latency_ms'] = {
            'catalog': percentile(catalog),
            'reader': percentile(reader_latency),
            'journal': percentile([j['journaled_at_micros'] / 1000 - j['commit_timestamp_micros'] / 1000
                                   for xid, j in journaled.items() if xid in workload and 'journaled_at_micros' in j]),
            'arrival_delay': percentile([t['arrival_delay_ms'] for t in report['transactions']]),
        }
        report['catalog_observations_complete'] = len(catalog) == len(report['transactions'])
        report['catalog_request_ms'] = {m: percentile([r['ms'] for r in report.get('catalog_http', [])
                                                       if r['method'] == m and r['kind'] == 'table'])
                                        for m in ('GET', 'POST')}
        if 'fault' in report:
            after = [t for t in report['transactions'] if t['sequence'] > report['fault']['after_sequence']]
            if after:
                report['fault']['first_post_fault_visible_seconds'] = (max(after[0]['reader_ms'].values()) - report['fault']['at_ms']) / 1000
        if metrics_samples:
            report['resources'] = {'peak_rss_mib': max(s['rss_mib'] for s in metrics_samples),
                                   'cpu_seconds': metrics_samples[-1]['cpu_seconds'] - metrics_samples[0]['cpu_seconds']}
        report['passed'] = True
    except BaseException as error:
        report['failure'] = str(error)
        report['traceback'] = traceback.format_exc()
        raise
    finally:
        stopped.set()
        for process in processes:
            if process.poll() is None:
                stop(process)
        if latency_proxy is not None:
            latency_proxy.__exit__()
        report['logs'] = [str(p) for p in logs]
        save()
        if pg is not None:
            pg.execute('SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=%s AND NOT active', [name])
            pg.close()
        summary = {k: v for k, v in report.items() if k not in ('transactions', 'reader_failures', 'traceback', 'catalog_http', 'events')}
        summary['reader_failures'] = len(report['reader_failures'])
        print(json.dumps(summary, indent=2, default=str), flush=True)


if __name__ == '__main__':
    main()
