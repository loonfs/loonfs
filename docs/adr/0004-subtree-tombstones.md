# ADR 0004: recursive delete uses subtree tombstones as the baseline

Status: accepted

We represent recursive directory delete with a subtree tombstone on the deleted directory root.

Consequences:
- recursive delete is cheap and correct
- descendants remain in history for undelete and audit
- async delete manifests may be added later, but generic delete vectors are not the baseline
