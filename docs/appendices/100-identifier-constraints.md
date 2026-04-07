# Appendix 100: Identifier Constraints

This appendix records product-facing identifier validation rules that are important for
interoperability, but are not themselves metadata-model invariants.

## 1. Namespace identifiers

LoonFS exposes two namespace-facing identifiers:

- `namespace_id`, the stable generated durable identifier; and
- `name`, the current human-facing namespace display name.

`namespace_id` is the canonical durable identity. `name` is a mutable selector resolved through the
namespace catalog.

### 1.1 `namespace_id`

In v0, generated namespace ids use the shape:

```text
ns_<32 lowercase hex characters>
```

Example:

```text
ns_0123456789abcdef0123456789abcdef
```

The `ns_` prefix is reserved for generated namespace ids.

### 1.2 Namespace `name`

In v0, namespace names must satisfy all of the following:

- length `1..=128`
- characters limited to ASCII letters, digits, `.`, `_`, and `-`
- must not use the reserved `ns_` prefix, case-insensitively

Representative regex:

```text
[A-Za-z0-9._-]{1,128}
```

Additional selector rule:

- names beginning with `ns_` remain invalid even if they match the character set, because that
  prefix is reserved for the stable namespace-id space

Namespace-name lookup is case-insensitive. The catalog stores and resolves a normalized lowercase
lookup key while preserving the current display form.

## 2. Filesystem item names

This appendix does not impose a global character-set restriction on file or directory display
names.

Filesystem item names are constrained by:

- path syntax, which forbids `/` inside one path component and rejects `.` and `..` as standalone
  components; and
- the namespace `NamePolicy`, which governs sibling-name comparison and collision detection

In v0, sibling-name comparison uses `nfc_casefold_v0`.
