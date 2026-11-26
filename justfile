# List available recipes.
default:
    @just --list

# Run local Rust quality, doc, test, and supply-chain checks (default & --all-features).
check:
    cargo fmt --all -- --check
    cargo clippy --all-targets --locked -- -D warnings
    cargo clippy --all-targets --all-features --locked -- -D warnings
    RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --locked
    RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features --locked
    cargo test --locked
    cargo test --all-features --locked
    cargo deny --all-features check
    cargo audit -D warnings

# Apply rustfmt across the project.
fmt:
    cargo fmt --all

# Run the sidecar locally with mainnet defaults pointing at localhost.
run:
    cargo run --release -- \
        --listen 127.0.0.1:8547 \
        --el-http-url http://127.0.0.1:8545 \
        --el-ws-url ws://127.0.0.1:8546 \
        --cl-beacon-url http://127.0.0.1:5052 \
        --network mainnet

# Dry-run a release (no changes applied). Levels: patch | minor | major | X.Y.Z
release-dry level='patch':
    cargo release {{level}}

# Execute a release (bumps Cargo.toml/.lock, commits, tags, pushes; CI builds binaries).
release level='patch':
    cargo release {{level}} --execute

# Run cargo-deny supply-chain checks.
deny:
    cargo deny --all-features check

# HTML coverage report under target/llvm-cov/html/.
coverage:
    cargo llvm-cov --all-features --locked --html
