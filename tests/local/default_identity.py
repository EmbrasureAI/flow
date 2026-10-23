#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "psycopg[binary]==3.3.5", "boto3==1.43.88", "fastavro==1.12.2"]
# ///
"""DEFAULT replica identity qualification against disposable services.

Runs against any disposable PostgreSQL/REST/MinIO services, such as the local
or production Compose fixtures.
"""
import argparse
import json
import os
import signal
import sys
from pathlib import Path
import subprocess
import time
import traceback
from types import SimpleNamespace

from run import Run, dump, lsn

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'production'))
from proxy import CatalogProxy


class DefaultRun(Run):
    def __init__(self, args):
        super().__init__(args)
        self.proxy = CatalogProxy(args.catalog_uri, self.directory / 'catalog-proxy.jsonl')
        # Only daemon requests use the gate. Row assertions read the real catalog.
        self.config.write_text(self.config.read_text().replace(
            f'uri = {json.dumps(args.catalog_uri)}', f'uri = {json.dumps(self.proxy.url)}'))

    def configure(self):
        super().configure()
        text = self.config.read_text().split('[[tables]]')[0]
        text = text.replace('chunk_bytes = 65536', 'chunk_bytes = 1048576').replace('batch_bytes = 262144', 'batch_bytes = 2097152')
        tables = [('orders', [('tenant', 'Int32', False), ('body', 'Int32', True), ('id', 'Int64', False)], [2, 0]),
                  ('accounts', [('id', 'Int64', False), ('body', 'String', False)], [0]),
                  ('projected', [('id', 'Int64', False), ('body', 'Int32', True)], [0]),
                  ('documents', [('id', 'Int64', False), ('body', 'String', False), ('version', 'Int32', False)], [0])]
        for name, columns, key in tables:
            text += f'\n[[tables]]\nsource_namespace = "{self.name}"\nsource_table = "{name}"\ntarget_namespace = ["{self.name}"]\ntarget_table = "{name}"\nprimary_key = {key}\n'
            if name == 'projected':
                text += 'column_selection = "explicit"\n'
            text += 'columns = [\n'
            for i, (col, kind, nullable) in enumerate(columns, 1):
                text += f'{{field_id = {i}, name = "{col}", data_type = "{kind}", nullable = {str(nullable).lower()}}},\n'
            text += ']\n'
        self.config.write_text(text)

    def seed(self):
        self.pg.execute(f'CREATE SCHEMA {self.name}')
        self.pg.execute('CREATE TABLE orders (tenant int NOT NULL, body int, id bigint NOT NULL, PRIMARY KEY(id,tenant))')
        self.pg.execute('CREATE TABLE accounts (id bigint PRIMARY KEY, body text NOT NULL)')
        self.pg.execute('ALTER TABLE accounts REPLICA IDENTITY FULL')
        self.pg.execute('ALTER TABLE accounts ALTER COLUMN body SET STORAGE EXTERNAL')
        self.pg.execute('CREATE TABLE projected (id bigint PRIMARY KEY, body int, excluded text)')
        # DEFAULT identity with externally stored text: updates that leave the
        # body unchanged carry it forward from the previous row.
        self.pg.execute('CREATE TABLE documents (id bigint PRIMARY KEY, body text NOT NULL, version int NOT NULL)')
        self.pg.execute('ALTER TABLE documents ALTER COLUMN body SET STORAGE EXTERNAL')
        self.pg.execute('INSERT INTO orders SELECT i % 13, i, i FROM generate_series(1,10000) i')
        # Incompressible, externally stored values; updates leave the value unchanged.
        self.pg.execute("INSERT INTO accounts SELECT i, (SELECT string_agg(md5(v::text), '') FROM generate_series(1,4096) v) FROM generate_series(1,3) i")
        self.pg.execute("INSERT INTO projected VALUES (1,10,repeat('excluded',10000))")
        self.pg.execute("INSERT INTO documents SELECT i, (SELECT string_agg(md5((v * i)::text), '') FROM generate_series(1,4096) v), 0 FROM generate_series(1,3) i")
        self.pg.execute(f'CREATE PUBLICATION {self.name} FOR TABLE orders, accounts, projected, documents')
        return {'server_version_num': self.pg.execute("SHOW server_version_num").fetchone()[0]}

    def compare(self, phase):
        results = {}
        for table, projection, order in [('orders','*','id,tenant'), ('accounts','*','id'), ('projected','id,body','id'),
                                         ('documents','*','id')]:
            expected = self.pg.execute(f'SELECT {projection} FROM {table} ORDER BY {order}').fetchall()
            metadata = self.table(table)
            actual = self.duck.execute(f'SELECT * FROM iceberg_scan(?) ORDER BY {order}', [metadata['metadata-location']]).fetchall()
            assert actual == expected, f'{phase}: {table}: expected {len(expected)} rows; got {len(actual)}; values differ'
            results[table] = {'rows':len(actual), 'snapshot':metadata['metadata']['current-snapshot-id']}
        return results

    def initialize(self):
        baseline = {table: self.pg.execute(f'SELECT {projection} FROM {table} ORDER BY id').fetchall()
                    for table, projection in [('orders','*'), ('accounts','*'), ('projected','id,body'), ('documents','*')]}
        with (self.directory/'init.log').open('wb') as log:
            process = subprocess.Popen(self.command('init'), env=self.environment, stdout=log, stderr=subprocess.STDOUT)
            try:
                def ready():
                    assert process.poll() in (None,0), 'initialization failed; see init.log'
                    return self.pg.execute('SELECT 1 FROM pg_replication_slots WHERE slot_name=%s AND confirmed_flush_lsn IS NOT NULL', (self.name,)).fetchone()
                self.until('slot not created', ready)
                self.handoff_barrier = self.transaction(['INSERT INTO orders VALUES (99,7,20001)',
                    'UPDATE orders SET body=body+1 WHERE id<100', 'DELETE FROM orders WHERE id=100',
                    'UPDATE accounts SET id=11 WHERE id=1', 'UPDATE projected SET body=20 WHERE id=1',
                    'UPDATE documents SET version=1 WHERE id=1'])
                assert process.wait(timeout=self.args.timeout) == 0, 'initialization failed; see init.log'
            finally:
                if process.poll() is None:
                    process.kill(); process.wait()
        for table, expected in baseline.items():
            assert self.rows(table) == expected, f'{table}: exported snapshot differs from its pre-write rows'
        return {table: len(rows) for table, rows in baseline.items()}

    def mutations(self):
        self.wait_materialized(self.transaction([
            'INSERT INTO orders VALUES (1,2,30000)',
            'UPDATE orders SET body=NULL WHERE id=30000',
            'UPDATE orders SET tenant=2,id=30001 WHERE id=30000',
            'DELETE FROM orders WHERE id=30001',
            'INSERT INTO orders VALUES (2,4,30001)',
            'UPDATE orders SET body=9 WHERE id=30001',
            'UPDATE orders SET tenant=99,id=30002 WHERE id=30001',
            'DELETE FROM orders WHERE id=200',
            'UPDATE accounts SET id=21 WHERE id=2',
            'DELETE FROM accounts WHERE id=3',
            'UPDATE projected SET body=NULL WHERE id=1',
            'UPDATE documents SET version=version+1 WHERE id=2',
            'UPDATE documents SET version=version+1 WHERE id=2',
        ]))
        self.wait_materialized(self.transaction([
            "INSERT INTO documents VALUES (4, (SELECT string_agg(md5((v * 4)::text), '') FROM generate_series(1,4096) v), 0)",
            'UPDATE documents SET version=9 WHERE id=4',
            'UPDATE documents SET version=version+1 WHERE id=1',
        ]))
        self.wait_materialized(self.transaction(['DELETE FROM orders WHERE id=30002',
            'INSERT INTO orders VALUES (99,42,30002)', 'UPDATE orders SET body=43 WHERE id=30002']))
        try:
            with self.pg.transaction():
                self.pg.execute('DELETE FROM orders')
                self.pg.execute('UPDATE accounts SET body=\'rollback\'')
                raise RuntimeError('deliberate rollback')
        except RuntimeError:
            pass
        self.wait_materialized(0)
        return self.compare('mutations-rollback')

    def restart(self):
        def source_fence():
            return lsn(self.pg.execute(
                "SELECT pg_logical_emit_message(true, 'flow-crash-barrier', '')::text"
            ).fetchone()[0])
        self.wait_materialized(source_fence())
        before_rows = self.rows('orders')
        offset = len(self.proxy.events)
        self.proxy.hold_commits(table='orders')
        try:
            batch_lsn = self.transaction(['UPDATE orders SET body=body+1000',
                                         'UPDATE accounts SET id=id+100', 'UPDATE projected SET body=88',
                                         'UPDATE documents SET version=version+100'])
            batch_fence = source_fence()
            self.until('no prepared orders commit reached the catalog gate', self.proxy.commit_held.is_set)
            # Prove this committed batch is durably captured, but not applied.
            def captured():
                metrics = self.metrics()
                return metrics if metrics.get('flow_journal_durable_lsn', 0) >= batch_fence else None
            pending = self.until('held batch was not durably captured', captured)
            confirmed = lsn(self.pg.execute(
                'SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=%s',
                (self.name,)).fetchone()[0])
            assert pending['flow_materialized_lsn'] < batch_lsn
            assert pending['flow_pending_transactions'] > 0
            assert confirmed < batch_lsn
            assert self.rows('orders') == before_rows, 'held batch already changed Iceberg rows'
            evidence = {'batch_lsn': batch_lsn, 'batch_fence': batch_fence,
                        'journal_durable_lsn': pending['flow_journal_durable_lsn'],
                        'materialized_lsn': pending['flow_materialized_lsn'],
                        'confirmed_lsn': confirmed, 'pending_transactions': pending['flow_pending_transactions']}
            dump(self.directory / 'pending-before-sigkill.json', evidence)
            process = self.process
            self.stop(crash=True)
            assert process.returncode == -signal.SIGKILL
        finally:
            # The real catalog may commit after the caller dies. Recovery must
            # recognize that same prepared operation rather than duplicate it.
            self.proxy.release_commits.set()
        completed = self.until('held commit did not complete after SIGKILL', lambda: [
            event for event in self.proxy.events[offset:] if event.get('held_before_upstream')
            and 200 <= event.get('upstream_status', 0) < 300])
        assert len(completed) == 1 and completed[0].get('operation_id')
        self.start()
        recovered = self.wait_materialized(batch_fence)
        first_rows = self.compare('interrupted-batch-recovery')
        snapshots = self.table('orders')['metadata']['snapshots']
        assert sum(s['summary'].get('flow.operation-id') == completed[0]['operation_id']
                   for s in snapshots) == 1, 'recovery duplicated the prepared commit'
        def acknowledged():
            value = lsn(self.pg.execute(
                'SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=%s',
                (self.name,)).fetchone()[0])
            return value if value >= batch_fence else None
        recovered_ack = self.until('source ACK did not advance after recovery', acknowledged)
        assert recovered['flow_materialized_lsn'] > evidence['materialized_lsn']
        # Keep the separate WAL-backlog restart case, with its own LSN.
        self.stop(crash=True)
        backlog_lsn = self.transaction(['DELETE FROM orders WHERE id%11=0',
                                       'UPDATE orders SET id=id+50000 WHERE id%5=0',
                                       'INSERT INTO orders VALUES (1,700,90000)',
                                       'UPDATE documents SET version=version+1'])
        self.start(); self.wait_materialized(backlog_lsn)
        result = self.compare('backlog-recovery')
        return {'pending_before_kill': evidence, 'held_commit': completed[0],
                'recovered_materialized_lsn': recovered['flow_materialized_lsn'],
                'recovered_confirmed_lsn': recovered_ack, 'interrupted_batch_rows': first_rows,
                'backlog_lsn': backlog_lsn, 'rows': result}

    def schema_change(self):
        self.pg.execute('ALTER TABLE orders ADD COLUMN extra integer')
        self.wait_materialized(self.transaction(['UPDATE orders SET extra=17 WHERE id=1']))
        return self.compare('nullable-fixed-addition')

    def unsupported_change(self, identity=False):
        # On a new run, fail one table after initial progress. Neither its target
        # nor the source-wide materialized/slot checkpoint may cross this batch.
        before = self.rows('orders')
        if identity:
            self.pg.execute('ALTER TABLE orders REPLICA IDENTITY NOTHING')
        else:
            # Nullable additions now evolve; a non-null backfill remains unsupported.
            self.pg.execute("ALTER TABLE orders ADD COLUMN unsafe text NOT NULL DEFAULT 'backfilled'")
        barrier = self.transaction(['INSERT INTO orders (tenant,body,id) VALUES (33,77,999999)'])
        table_id = self.pg.execute("SELECT 'orders'::regclass::oid").fetchone()[0]
        self.until('table did not quarantine', lambda: any(t['table_id'] == table_id
            for t in json.loads((self.directory/'state'/'status.json').read_text()).get('blocked_tables', [])))
        time.sleep(2)
        assert self.rows('orders') == before
        metrics = self.metrics()
        confirmed = lsn(self.pg.execute('SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=%s',(self.name,)).fetchone()[0])
        assert confirmed < barrier and metrics['flow_materialized_lsn'] < barrier, (confirmed,barrier,metrics)
        self.stop(crash=True); self.start()
        time.sleep(2)
        assert self.rows('orders') == before
        after_restart = lsn(self.pg.execute('SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=%s',(self.name,)).fetchone()[0])
        assert after_restart < barrier
        return {'failed_batch_lsn':barrier,'confirmed_lsn':confirmed,'metrics':metrics}

    def execute(self):
        try:
            self.phase('seed',self.seed)
            self.phase('snapshot-with-concurrent-writes',self.initialize)
            self.start()
            self.phase('handoff',self.handoff)
            if self.args.identity_only:
                self.phase('identity-change-checkpoint', lambda: self.unsupported_change(identity=True))
            else:
                self.phase('mutations-and-rollback',self.mutations)
                self.phase('crash-and-replay',self.restart)
                self.phase('fixed-column-addition',self.schema_change)
                self.phase('unsupported-schema-checkpoint',self.unsupported_change)
            self.report['passed']=True
        except BaseException as error:
            self.report.update(failure=str(error),traceback=traceback.format_exc())
            raise
        finally:
            self.stop(); self.proxy.close(); dump(self.directory/'report.json',self.report)
            self.pg.close(); self.duck.close()


def main():
    # Same arguments as the sibling tests/local scripts (see README.md).
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--postgres-url', default=os.environ.get('FLOW_POSTGRES_URL'))
    parser.add_argument('--catalog-uri', required=True)
    parser.add_argument('--s3-endpoint', required=True)
    parser.add_argument('--warehouse', default='s3://warehouse/')
    parser.add_argument('--binary', type=Path, default=Path('target/debug/embrasure-flow'))
    parser.add_argument('--artifacts', type=Path, required=True,
                        help='new directory; the run and identity-change cases use subdirectories')
    parser.add_argument('--timeout', type=float, default=180)
    args = parser.parse_args()
    if not args.postgres_url:
        parser.error('provide --postgres-url or FLOW_POSTGRES_URL')
    for case, identity_only in (('run', False), ('identity-change', True)):
        DefaultRun(SimpleNamespace(**(vars(args) | {'artifacts': args.artifacts / case,
                                                    'identity_only': identity_only}))).execute()
    print(f'PASS: {args.artifacts.resolve()}', flush=True)


if __name__ == '__main__':
    main()
