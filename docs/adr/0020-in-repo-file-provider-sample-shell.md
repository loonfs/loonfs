# ADR 0020: bring the first File Provider sample shell in repo once the Rust bridge is stable

Status: accepted

## Decision

The first runnable macOS File Provider sample now lives in this repo as a developer-only app and
extension shell under `native/macos/LoonFileProviderSample/`.

Rules:

- native logic still calls `loon-macos` through the existing C ABI + JSON bridge
- the containing app owns:
  - sample config loading
  - domain registration
  - domain removal/reset
- the extension owns:
  - enumeration
  - item lookup
  - targeted hydration
- the sample remains read-only and full-snapshot only
- repo-safe native logic and tests live in a Swift package inside the sample directory
- Finder packaging stays in the Xcode project inside that same sample directory
- ordinary workspace validation must remain cargo-safe; full-Xcode validation is opt-in/manual

## Consequences

- the native sample can evolve next to the Rust bridge without drifting out of sync
- the repo now carries Xcode project files, plist files, entitlements, and Swift sources for the
  developer sample
- the bridge boundary remains stable and reviewable through the checked-in C header and Swift
  wrapper code
- the sample can prove real Finder integration without widening into write support or background
  sync automation
