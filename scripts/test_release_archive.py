"""Regression tests for the release gate, including missing and altered notices."""
import io
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
import zipfile

ROOT = Path(__file__).resolve().parent.parent


class ReleaseArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.notices = self.directory / "notices"
        self.notices.mkdir()
        self.files = {name: (ROOT / name).read_bytes() for name in ("LICENSE", "LICENSE-MIT", "LICENSE-APACHE")}
        for name in ("THIRD_PARTY_LICENSES.txt", "RUST_LIBRARY_LICENSES.html"):
            content = f"test fixture for {name}\n".encode()
            (self.notices / name).write_bytes(content)
            self.files[name] = content

    def check_archive(self, suffix, files):
        path = self.directory / ("release" + suffix)
        if suffix == ".zip":
            with zipfile.ZipFile(path, "w") as archive:
                for name, data in files.items():
                    archive.writestr(name, data)
            target = "x86_64-pc-windows-msvc"
        else:
            with tarfile.open(path, "w:gz") as archive:
                for name, data in files.items():
                    member = tarfile.TarInfo("./" + name)
                    member.size = len(data)
                    archive.addfile(member, io.BytesIO(data))
            target = "aarch64-unknown-linux-musl"
        try:
            return subprocess.run(
                [sys.executable, str(ROOT / "scripts/check-release-archive.py"),
                 "--archive", str(self.directory / "release"), "--target", target,
                 "--licenses-dir", str(self.notices)],
                capture_output=True, text=True, timeout=10,
            )
        finally:
            path.unlink()

    def archives(self):
        for suffix, binary in ((".tar.gz", "ethonyx"), (".zip", "ethonyx.exe")):
            yield suffix, {**self.files, binary: b"nonempty test binary"}

    def test_complete_archives_pass(self):
        for suffix, files in self.archives():
            with self.subTest(suffix=suffix):
                result = self.check_archive(suffix, files)
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_every_required_file_is_mandatory(self):
        for suffix, files in self.archives():
            for omitted in files:
                with self.subTest(suffix=suffix, omitted=omitted):
                    result = self.check_archive(suffix, {name: data for name, data in files.items() if name != omitted})
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn(omitted, result.stderr)

    def test_changed_or_empty_notices_fail(self):
        for suffix, files in self.archives():
            for name in self.files:
                for content in (b"", files[name] + b"altered"):
                    with self.subTest(suffix=suffix, name=name, content=content[-10:]):
                        result = self.check_archive(suffix, {**files, name: content})
                        self.assertNotEqual(result.returncode, 0)
                        self.assertIn(name, result.stderr)

    def test_empty_binary_fails(self):
        for suffix, files in self.archives():
            binary = "ethonyx.exe" if suffix == ".zip" else "ethonyx"
            with self.subTest(suffix=suffix):
                result = self.check_archive(suffix, {**files, binary: b""})
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(binary, result.stderr)


if __name__ == "__main__":
    unittest.main()
