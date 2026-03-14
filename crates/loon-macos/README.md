# loon-macos

Reserved macOS integration surface.

The design intent stays the same: a later File Provider bridge should layer on top of the same
client semantics rather than inventing a new sync model. This crate is intentionally quarantined
until the underlying client/server delivery surfaces are further along.
