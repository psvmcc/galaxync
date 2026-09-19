# Security policy

## Supported versions

Security fixes are applied to the latest released version and to the `main`
branch. Older versions may need to be upgraded before receiving a fix.

## Reporting a vulnerability

Please do not open a public issue for a suspected vulnerability. Use GitHub's
private vulnerability reporting for
[`psvmcc/galaxync`](https://github.com/psvmcc/galaxync/security/advisories/new).

Include the affected version, configuration, reproduction steps, impact, and
any suggested mitigation. Secrets and private Galaxy URLs should be redacted.
You should receive an acknowledgement within seven days. A fix and disclosure
timeline will be coordinated after the report is validated.

## Operational scope

Operators remain responsible for protecting API tokens, restricting access to
the storage directory and metrics listener, and updating the daemon and its
container base promptly.
