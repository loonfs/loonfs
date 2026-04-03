# Versioning and conformance

A consumable spec needs a clear answer to two questions:

1. what makes one durable object readable and trustworthy?
2. what has to be proven before an implementation may claim to implement LoonFS?

## Durable object envelopes

Every durable object format should carry at least:

- `kind`
- `format_version`
- `writer_version`
- `payload_checksum_sha256`

This applies to both JSON control objects and compact immutable objects such as WAL entries or checkpoint segments.

## Versioned decisions

The following choices are intentionally versioned because they shape interoperability:

| Versioned surface | Why |
| --- | --- |
| Durable object formats | Readers need to know how to decode and validate stored objects. |
| `NamePolicy` | Client and server must agree on collision rules. |
| Mutation schema | Writers and readers must agree on operation meaning and ordering. |
| HTTP API binding | Independent implementations need a stable request and response shape. |

Breaking changes to those surfaces require a spec update and an explicit version change.

## Conformance profiles

| Profile | Must prove |
| --- | --- |
| **Object-store provider** | The provider satisfies the object-store contract, including conditional writes and strong visibility. |
| **Authoritative writer** | The writer reconstructs basis state correctly, enforces preconditions, writes WAL before head publish, and preserves one-request-one-`seq` semantics. |
| **Reader** | The reader reconstructs visible state correctly from head, checkpoint, and WAL. |
| **Background worker** | The worker publishes checkpoints and progress safely and does not regress monotonic coverage. |
| **Client** | The client preserves inode identity, upload-before-publish behavior, conflict preservation, and restart safety. |

## Compatibility rule of thumb

A change belongs in the core spec only when independent implementations must agree on it to interoperate safely.

A change does **not** belong in the core spec merely because the reference implementation currently does it that way.

## What should remain outside the core spec

These documents are important, but they should be treated as implementation notes or product guides rather than the public core spec:

- testing strategy
- repository layout and delivery planning
- milestone notes
- platform spike documents
- CLI ergonomics
- one reference client’s internal schema evolution

Keeping those out of the core spec is part of what keeps the core spec readable.
