# Third-party notices

OddsFox Execution is distributed under the MIT License in `LICENSE`.

The live adapter uses the official
`polymarket_client_sdk_v2` crate, pinned to version `0.7.0`. That SDK declares
the MIT License:

```text
Copyright (c) 2025-2026 Polymarket
```

Its MIT license permits use, modification, distribution, sublicensing, and
sale subject to preserving its copyright and permission notice. The complete
license text is included in the SDK source package. Official container images
include this notice and the project's MIT license under
`/usr/share/licenses/oddsfox-execution/`.

Other Rust dependencies and their licenses are governed by `Cargo.lock` and
the allow/deny policy in `deny.toml`. Release SBOMs identify the exact
dependency versions and declared licenses; this file is not a substitute for
those machine-readable inventories.
