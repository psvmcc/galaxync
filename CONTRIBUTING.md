# Contributing

Contributions are welcome through GitHub issues and pull requests.

## Development

Install the Rust toolchain selected by `rust-toolchain.toml` and
[`just`](https://github.com/casey/just), then run:

```shell
just check
```

Changes should include tests for new behavior and update `README.md` and the
example configuration when they change user-facing behavior. Do not commit API
tokens, generated collection archives, runtime state, or local configuration.

## Pull requests

- Keep each pull request focused and explain its operational impact.
- Preserve backward compatibility for configuration and on-disk state, or
  document a migration.
- Ensure formatting, Clippy, tests, dependency policy, and documentation checks
  pass.
- By submitting a contribution, you agree that it is licensed under the MIT
  License used by this repository.
