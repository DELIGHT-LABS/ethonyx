# Native library notices

`musl-COPYRIGHT` is an unmodified copy of the musl 1.2.5 copyright notice:

https://git.musl-libc.org/cgit/musl/plain/COPYRIGHT?h=v1.2.5

Rust 1.99 builds its musl standard libraries against musl 1.2.5:

https://github.com/rust-lang/rust/blob/1.99.1/src/ci/docker/scripts/musl.sh

The generator appends this notice only for Linux musl targets. Recheck this
file when upgrading Rust or changing the native linker/libc used for releases.
Rust's own library notices are copied from the active toolchain at generation
time. Native code bundled inside Cargo packages, such as AWS-LC, is covered by
collecting the packages' original license and notice files recursively.
