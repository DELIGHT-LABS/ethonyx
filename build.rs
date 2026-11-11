//! Capture the git description at build time so logs can identify the exact
//! commit a binary was built from — distinguishing a tagged release (`v0.1.2`)
//! from an ad-hoc build off some commit (`v0.1.2-5-g20537f9`, `-dirty` if the
//! tree had uncommitted changes). Falls back to `unknown` when git isn't
//! available (e.g. building from a source tarball).

use std::process::Command;

fn main() {
    let describe = Command::new("git")
        .args(["describe", "--always", "--dirty", "--tags"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=ETHONYX_GIT_DESCRIBE={describe}");

    // Rebuild when HEAD moves or the index changes, so the value stays current.
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/index");
}
