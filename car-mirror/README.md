<div align="center">
  <a href="https://github.com/fission-codes/rs-car-mirror" target="_blank">
    <img src="https://raw.githubusercontent.com/fission-codes/rs-car-mirror/main/assets/a_logo.png" alt="car-mirror Logo" width="100"></img>
  </a>

  <h1 align="center">car-mirror</h1>

  <p>
    <a href="https://crates.io/crates/car-mirror">
      <img src="https://img.shields.io/crates/v/car-mirror?label=crates" alt="Crate">
    </a>
    <a href="https://codecov.io/gh/fission-codes/rs-car-mirror">
      <img src="https://codecov.io/gh/fission-codes/rs-car-mirror/branch/main/graph/badge.svg?token=SOMETOKEN" alt="Code Coverage"/>
    </a>
    <a href="https://github.com/fission-codes/rs-car-mirror/actions?query=">
      <img src="https://github.com/fission-codes/rs-car-mirror/actions/workflows/tests_and_checks.yml/badge.svg" alt="Build Status">
    </a>
    <a href="https://github.com/fission-codes/rs-car-mirror/blob/main/LICENSE-APACHE">
      <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License-Apache">
    </a>
    <a href="https://github.com/fission-codes/rs-car-mirror/blob/main/LICENSE-MIT">
      <img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License-MIT">
    </a>
    <a href="https://docs.rs/car-mirror">
      <img src="https://img.shields.io/static/v1?label=Docs&message=docs.rs&color=blue" alt="Docs">
    </a>
    <a href="https://discord.com/invite/zAQBDEq">
      <img src="https://img.shields.io/static/v1?label=Discord&message=join%20us!&color=mediumslateblue" alt="Discord">
    </a>
  </p>
</div>

<div align="center"><sub>:warning: Work in progress :warning:</sub></div>

## car-mirror

This is a [sans-io] implementation of the [car mirror protocol].
Car mirror is used to transfer [IPLD] from one computer to another over the internet over various transports,
but most notably HTTP(s).
It tries to do so with deduplication and minimal communcation round-trips.

The main storage abstraction that this crate uses is a [`BlockStore`] implementation from the [`wnfs-common` crate].

If you're looking for higher-level APIs, please take a look at `car-mirror-reqwest` for clients or `car-mirror-axum` for servers.

## License

This project is licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](./LICENSE-APACHE) or [http://www.apache.org/licenses/LICENSE-2.0][apache])
- MIT license ([LICENSE-MIT](./LICENSE-MIT) or [http://opensource.org/licenses/MIT][mit])

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.


[apache]: https://www.apache.org/licenses/LICENSE-2.0
[mit]: http://opensource.org/licenses/MIT
[car mirror protocol]: https://github.com/wnfs-wg/car-mirror-spec
[IPLD]: https://ipld.io
[sans-io]: https://sans-io.readthedocs.io/
[`BlockStore`]: https://docs.rs/wnfs-common/latest/wnfs_common/blockstore/trait.BlockStore.html
[`wnfs-common` crate]: https://docs.rs/wnfs-common/latest/wnfs_common/
