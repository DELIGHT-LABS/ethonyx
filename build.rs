//! Capture the git description at build time so logs can identify the exact
//! commit a binary was built from — distinguishing a tagged release (`v0.1.2`)
//! from an ad-hoc build off some commit (`v0.1.2-5-g20537f9`, `-dirty` if the
//! tree had uncommitted changes). Falls back to `unknown` when git isn't
//! available (e.g. building from a source tarball).

use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};

fn git_stdout(args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(args)
        .stderr(Stdio::null())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok()
}

fn emit_if_exists(path: &Path) {
    if path.exists() {
        println!("cargo::rerun-if-changed={}", path.display());
    }
}

/// Watch Rust sources individually (a directory watch on `src` would also fire
/// on editor swap files).
fn emit_source_rerun_hints(dir: &Path) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        let path = entry.path();
        if file_type.is_dir() {
            emit_source_rerun_hints(&path);
        } else if file_type.is_file() && path.extension().is_some_and(|ext| ext == "rs") {
            println!("cargo::rerun-if-changed={}", path.display());
        }
    }
}

fn emit_git_rerun_hints() {
    // Resolve worktree-local (`HEAD`, `logs/HEAD`) and shared (`packed-refs`,
    // `refs/tags`, `--git-common-dir`) paths in one git call. Omit `index`:
    // `git describe --dirty` refreshes `.git/index` during `build.rs`, which
    // would trigger a spurious second rebuild; crate source hints in `main`
    // track uncommitted edits at zero extra compile cost.
    let Some(text) = git_stdout(&[
        "rev-parse",
        "--git-path",
        "HEAD",
        "--git-path",
        "logs/HEAD",
        "--git-path",
        "packed-refs",
        "--git-path",
        "refs/tags",
        "--git-common-dir",
    ]) else {
        return;
    };
    let mut lines = text.lines();
    let [
        Some(head),
        Some(logs_head),
        Some(packed_refs),
        Some(refs_tags),
        Some(common_dir),
    ] = std::array::from_fn(|_| lines.next())
    else {
        return;
    };
    if lines.next().is_some() {
        return;
    }

    for path in [head, logs_head, packed_refs, refs_tags] {
        emit_if_exists(Path::new(path));
    }

    let common_path = Path::new(common_dir);
    if let Ok(head_contents) = fs::read_to_string(head)
        && let Some(symref) = head_contents
            .strip_prefix("ref:")
            .map(str::trim)
            .filter(|s| {
                s.strip_prefix("refs/")
                    .is_some_and(|rest| !rest.is_empty() && !rest.contains(".."))
            })
        && let Some(watch_path) = common_path
            .join(symref)
            .ancestors()
            .take_while(|p| *p != common_path)
            .find(|p| p.exists())
    {
        println!("cargo::rerun-if-changed={}", watch_path.display());
    }
}

fn main() {
    let stdout = git_stdout(&["describe", "--always", "--dirty", "--tags"]);
    let describe = stdout
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("unknown");
    println!("cargo::rustc-env=ETHONYX_GIT_DESCRIBE={describe}");

    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rerun-if-changed=Cargo.toml");
    emit_if_exists(Path::new("Cargo.lock"));
    emit_source_rerun_hints(Path::new("src"));
    emit_git_rerun_hints();
}
