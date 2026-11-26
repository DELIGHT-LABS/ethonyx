## Summary

<!-- What changed and why? Keep it concise. -->

## Type of change

- [ ] Bug fix (non-breaking)
- [ ] New feature (non-breaking)
- [ ] Breaking change
- [ ] Documentation / chore / CI

## Test plan

<!-- How was this verified locally? -->

## Checklist

- [ ] `cargo fmt --all -- --check` clean
- [ ] `cargo clippy --all-targets --locked -- -D warnings` clean (default & `--all-features`)
- [ ] `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --locked` clean (default & `--all-features`)
- [ ] `cargo test --locked` passes (default & `--all-features`)
- [ ] `cargo deny --all-features check` and `cargo audit -D warnings` clean
- [ ] `CHANGELOG.md` updated under `[Unreleased]` if user-visible
