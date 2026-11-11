#!/usr/bin/env python3
"""Verify release archives contain the binary and the exact generated notices."""

import argparse
from pathlib import Path
import tarfile
import zipfile


ROOT = Path(__file__).resolve().parent.parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", required=True, type=Path, help="Archive basename, without extension")
    parser.add_argument("--target", required=True)
    parser.add_argument("--licenses-dir", type=Path, default=ROOT / "target/release-licenses")
    args = parser.parse_args()
    expected = {
        "LICENSE": (ROOT / "LICENSE").read_bytes(),
        "LICENSE-MIT": (ROOT / "LICENSE-MIT").read_bytes(),
        "LICENSE-APACHE": (ROOT / "LICENSE-APACHE").read_bytes(),
        "THIRD_PARTY_LICENSES.txt": (args.licenses_dir / "THIRD_PARTY_LICENSES.txt").read_bytes(),
        "RUST_LIBRARY_LICENSES.html": (args.licenses_dir / "RUST_LIBRARY_LICENSES.html").read_bytes(),
    }
    binary = "ethonyx.exe" if "-windows-" in args.target else "ethonyx"
    archives = [Path(str(args.archive) + suffix) for suffix in (".tar.gz", ".zip")]
    archives = [path for path in archives if path.is_file()]
    if not archives:
        raise RuntimeError(f"No release archives found for {args.archive}")

    for path in archives:
        if path.suffix == ".zip":
            with zipfile.ZipFile(path) as archive:
                contents = {name: archive.read(name) for name in archive.namelist()}
        else:
            with tarfile.open(path, "r:gz") as archive:
                contents = {
                    member.name.removeprefix("./"): archive.extractfile(member).read()
                    for member in archive.getmembers() if member.isfile()
                }
        if not contents.get(binary):
            raise RuntimeError(f"{path}: missing or empty {binary}")
        for name, original in expected.items():
            if not original or contents.get(name) != original:
                raise RuntimeError(f"{path}: missing, empty, or changed {name}")
        print(f"Verified binary and license notices in {path}")


if __name__ == "__main__":
    main()
