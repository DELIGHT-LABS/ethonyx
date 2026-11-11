#!/usr/bin/env python3
"""Generate the notices shipped alongside a release binary (Python 3.9+)."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parent.parent
NOTICE_PREFIXES = ("LICENSE", "LICENCE", "COPYING", "COPYRIGHT", "NOTICE")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True)
    parser.add_argument("--features", default="")
    parser.add_argument("--all-features", action="store_true")
    parser.add_argument("--no-default-features", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "target/release-licenses")
    args = parser.parse_args()

    command = [
        "cargo", "about", "generate", "--locked", "--fail", "--format", "json",
        "--target", args.target,
    ]
    if args.features:
        command += ["--features", args.features]
    if args.all_features:
        command += ["--all-features"]
    if args.no_default_features:
        command += ["--no-default-features"]
    # cargo-about rejects redirected stdout under PowerShell. Its explicit
    # output file also avoids shell-dependent encoding on Windows runners.
    with tempfile.TemporaryDirectory(prefix="ethonyx-licenses-") as temporary:
        report_path = Path(temporary) / "licenses.json"
        subprocess.run(command + ["--output-file", str(report_path)], cwd=ROOT, check=True)
        report = json.loads(report_path.read_text(encoding="utf-8"))
    if not report["crates"] or not report["licenses"]:
        raise RuntimeError("cargo-about returned an empty license report")

    rustc = subprocess.check_output(["rustc", "--version"], cwd=ROOT, text=True).strip()
    sysroot = Path(subprocess.check_output(
        ["rustc", "--print", "sysroot"], cwd=ROOT, text=True,
    ).strip())
    # Supplied by rustc itself, including with rustup's minimal profile.
    rust_notices = (sysroot / "share/doc/rust/COPYRIGHT-library.html").read_bytes()
    if not rust_notices:
        raise RuntimeError("Rust library copyright notice is empty")

    sections = [
        "ETHONYX THIRD-PARTY LICENSES\n============================",
        "Ethonyx is licensed under MIT OR Apache-2.0, at your option. See LICENSE,\n"
        "LICENSE-MIT and LICENSE-APACHE. Dependencies retain their own licenses.\n"
        "This report includes the target's enabled dependencies and build dependencies;\n"
        "development-only dependencies are excluded. Upstream license bundles may\n"
        "also cover code not linked into this binary. Rust library notices are in\n"
        "RUST_LIBRARY_LICENSES.html.",
        f"Target: {args.target}\nToolchain: {rustc}\n"
        f"Features: {'all' if args.all_features else args.features or '(none explicitly enabled)'}\n"
        f"Default features enabled: {not args.no_default_features}\n"
        f"Cargo.lock SHA-256: {hashlib.sha256((ROOT / 'Cargo.lock').read_bytes()).hexdigest()}",
        "DEPENDENCY INVENTORY (upstream SPDX expressions)",
    ]
    packages = sorted(
        (item["package"] for item in report["crates"]),
        key=lambda package: (package["name"], package["version"]),
    )
    sections.append("\n".join(
        f"- {p['name']} {p['version']}: {p['license'] or 'see upstream license file'}"
        for p in packages
    ))
    sections.append("LICENSE TEXTS (alternatives selected by about.toml)")
    for license_info in report["licenses"]:
        users = "\n".join(
            f"- {user['crate']['name']} {user['crate']['version']}"
            for user in license_info["used_by"]
        )
        sections.append(
            f"{license_info['name']} ({license_info['id']})\nUsed by:\n{users}\n\n"
            + license_info["text"]
        )

    # Preserve original copyright holders, native bundles (e.g. AWS-LC), and
    # NOTICE files as well: SPDX fallback text alone can omit attribution.
    sections.append("ORIGINAL UPSTREAM LICENSE AND NOTICE FILES")
    for package in packages:
        source = Path(package["manifest_path"]).parent
        if source.resolve() == ROOT:
            continue
        for path in sorted(source.rglob("*")):
            if path.is_file() and path.name.upper().startswith(NOTICE_PREFIXES):
                sections.append(
                    f"{package['name']} {package['version']} / {path.relative_to(source).as_posix()}\n\n"
                    + path.read_text(encoding="utf-8")
                )

    if "-linux-musl" in args.target:
        sections.append(
            "MUSL LIBC (outside the Cargo dependency graph)\n"
            "Source: https://git.musl-libc.org/cgit/musl/plain/COPYRIGHT?h=v1.2.5\n\n"
            + (ROOT / "licenses/musl-COPYRIGHT").read_text(encoding="utf-8")
        )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    separator = "\n\n" + "-" * 79 + "\n\n"
    (args.output_dir / "THIRD_PARTY_LICENSES.txt").write_text(
        separator.join(sections) + "\n", encoding="utf-8",
    )
    (args.output_dir / "RUST_LIBRARY_LICENSES.html").write_bytes(rust_notices)
    print(f"Generated notices for {len(packages)} packages in {args.output_dir}")


if __name__ == "__main__":
    main()
