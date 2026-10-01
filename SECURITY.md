# Security policy

## Reporting a vulnerability

Please do not report security vulnerabilities in public issues, discussions or
pull requests.

Report them privately through GitHub:
[**Report a vulnerability**](https://github.com/EmbrasureAI/flow/security/advisories/new)
(Security tab → Advisories → Report a vulnerability). Include:

- the affected version or commit;
- the component (source connection, catalog client, storage, HTTP listener,
  local state) and configuration involved, with credentials removed;
- steps to reproduce, and the impact you observed or expect.

We aim to acknowledge a report within three business days and to agree on a
fix and disclosure timeline with you. We credit reporters in the advisory
unless you prefer otherwise.

## Supported versions

Flow is in beta. Security fixes are made on `main` and released in the next
patch release of the latest minor version. Older minor versions do not receive
fixes; see [upgrading](docs/upgrading.md).

## Scope

In scope: vulnerabilities in this repository's code, its Dockerfile and
release artifacts, including credential exposure in logs or errors, TLS
verification, unauthorized access through the optional HTTP listener, and
state or data corruption an attacker can trigger.

## Deployment

Deployment choices are outside Flow's control but worth knowing:

- **Secrets.** The PostgreSQL URL comes from the environment variable named by
  `source.connection_env`, and REST catalog `token_env`/`credential_env` name
  environment variables. Literal `[catalog]` properties, including `token`,
  `credential` and object-store keys such as `s3.secret-access-key`, are also
  accepted, so protect `flow.toml` like any other secret when you use them.
  Without explicit keys, object-store access uses the default AWS credential
  chain (environment, shared profile files, web identity, container and
  instance metadata),
  so those sources are in scope for the process too. Protect the process
  environment, and keep `flow.toml` out of version control.
- **Local state.** The `state_dir` holds replicated row data in the journal,
  spool and index. Flow runs with a `0077` umask on Unix, so every file and
  directory it creates is readable only by the user running Flow. This includes
  files written to a `file://` warehouse, which other users therefore cannot
  read. Flow removes group and other access from an existing `state_dir` owned
  by that user, warns about one owned by root (such as a Kubernetes volume with
  `fsGroup`), and refuses to start when another non-root user owns it and group
  or other users can write to it. On shared volumes, point `state_dir` at a
  subdirectory Flow creates, such as `/data/state`, to avoid both. Readers of
  `metrics.prom` or `status` must run as the Flow user; otherwise use the HTTP
  listener. Backups of `state_dir` contain row data.
- **Container image.** The image built from the Dockerfile runs as UID 10001 and its `/data` is `0700`.
  Runtimes that assign an arbitrary UID (such as OpenShift) cannot use it; mount
  a volume writable by that UID and set `state_dir` inside it.
- **PostgreSQL TLS.** Connection settings follow libpq, whose default
  `sslmode=prefer` silently falls back to plaintext. Across untrusted networks
  use `sslmode=verify-full`, which verifies the certificate chain and the
  server hostname, with `sslrootcert` naming the server's CA bundle (without it
  the system trust store is used). `sslmode=require` encrypts but does not
  authenticate the server unless `sslrootcert` is set. `sslmode=verify-ca` does
  not check the hostname, so any certificate issued by the configured CA is
  accepted; Flow requires `sslrootcert` for it and it is appropriate only for a
  private CA. `init`, `run` and `check --source` warn about unauthenticated
  settings for connections that leave the host.
- **Catalog and object store.** Prefer `https://` catalog and object-store
  endpoints across untrusted networks. Flow redacts catalog and storage
  credentials, including vended credentials, from its errors and from the
  debug output of catalog and storage configuration.
- **Logs.** Keep `RUST_LOG` at `info` in production. Flow drops debug and
  trace records from the AWS request signing crates (`reqsign*`) whatever
  `RUST_LOG` requests, because they print credentials, but other dependencies'
  debug and trace output can include queries, object paths and request details.
- **HTTP listener.** The optional `[http]` listener is unauthenticated and
  serves status and metrics. Bind it to a private interface. It accepts up to
  64 concurrent connections and closes clients that do not send a request
  within one second.
- **File descriptors.** RocksDB keeps index files open and each HTTP connection
  uses a descriptor, so raise the default 1024 soft limit, for example
  `LimitNOFILE=65536` in a systemd unit.
