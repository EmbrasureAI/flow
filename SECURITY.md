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

In scope: vulnerabilities in this repository's code and the published binaries
and container images, including credential exposure in logs or errors, TLS
verification, unauthorized access through the optional HTTP listener, and
state or data corruption an attacker can trigger.

Deployment choices are outside Flow's control but worth knowing:

- Flow reads credentials from environment variables named in the configuration.
  Protect the process environment and the `state_dir`, which contains replicated
  row data in the journal and spool.
- The optional `[http]` listener is unauthenticated. Bind it to a private
  interface.
- Use `sslmode=verify-full` (or `verify-ca`) for PostgreSQL connections that
  cross untrusted networks.
