# LoonFS Catalog Specification (Reserved)

This document name is reserved for a future companion specification and
intentionally contains no normative content today.

**Scope when it arrives:** directory-scoped catalog concerns that span stores
and tenants — cross-store namespace discovery, naming authority, search,
ownership, and quotas. This is deliberately separate from the store-scoped
namespace operations in `api.md` (list, create, fork, delete within one
configured store), which are data-plane features derivable from format
metadata. The split mirrors Apache Iceberg's separation of its table format
from its REST catalog.

Nothing in `format.md` or `api.md` assumes or precludes this document.
