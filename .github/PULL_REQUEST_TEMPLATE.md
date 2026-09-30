## Problem

<!-- What is wrong or missing, and who is affected? Link the issue if there is one. -->

## Change

<!-- The resulting behavior. Call out any change to configuration, on-disk state,
     acknowledgement, publication or recovery, and any upgrade action. -->

## Validation

<!-- Tests added or run. Changes to source acknowledgement, catalog recovery,
     index application or delete handling need failure-path coverage. -->

- [ ] `cargo fmt --all --check`
- [ ] `cargo clippy --locked --workspace --all-targets --no-deps -- -D warnings`
- [ ] `cargo test --locked --workspace`
- [ ] Documentation and `CHANGELOG.md` updated for user-visible changes
