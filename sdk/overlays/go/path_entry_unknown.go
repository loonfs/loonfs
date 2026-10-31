package loonfs

import "encoding/json"

// PathEntryUnknown contains the fields that every path entry includes, for an entry of an inode
// kind that this version of the SDK doesn't define (such as a kind that a newer server added).
type PathEntryUnknown struct {
	// The complete attribute map at `attributes_revision_no`. The map is empty before the first
	// update.
	Attributes *Attributes `json:"attributes,omitempty" url:"attributes,omitempty"`
	// The attribute revision that these attributes come from.
	AttributesRevisionNo *AttributesRevisionNo `json:"attributes_revision_no,omitempty" url:"attributes_revision_no,omitempty"`
	// The time of the latest attribute update, in Unix milliseconds. It's absent before the first
	// update.
	AttributesUpdatedAtMs *int64 `json:"attributes_updated_at_ms,omitempty" url:"attributes_updated_at_ms,omitempty"`
	// The actor responsible for the latest attribute update. It's absent before the first update.
	AttributesUpdatedBy *ActorID `json:"attributes_updated_by,omitempty" url:"attributes_updated_by,omitempty"`
	// The opaque ID of the current binding of the entry's parent and name. It's absent for the
	// namespace root.
	BindingVersion *BindingVersion `json:"binding_version,omitempty" url:"binding_version,omitempty"`
	// The time the inode was created, in Unix milliseconds.
	CreatedAtMs int64 `json:"created_at_ms" url:"created_at_ms"`
	// The actor that created this inode, as the application supplied it.
	CreatedBy ActorID `json:"created_by" url:"created_by"`
	// The stored display name of this path component. It's absent for the root, which has no name.
	DisplayName *DisplayName `json:"display_name,omitempty" url:"display_name,omitempty"`
	// The namespace head sequence that this response was read at.
	HeadSeq ChangeSeq `json:"head_seq" url:"head_seq"`
	// The entry's stable inode ID.
	InodeID InodeID `json:"inode_id" url:"inode_id"`
	// The namespace that was read.
	NamespaceID NamespaceID `json:"namespace_id" url:"namespace_id"`
	// The parent directory's inode. It's absent for the root.
	ParentInodeID *InodeID `json:"parent_inode_id,omitempty" url:"parent_inode_id,omitempty"`
	// The absolute path, built from the stored display names.
	Path AbsolutePath `json:"path" url:"path"`
}

// GetUnknown returns the fields that every path entry includes when the entry's inode kind is one
// that this version of the SDK doesn't define, and nil for a directory or a file entry. The API
// requires a client to keep such an entry, read those fields, and ignore the fields that belong to
// the unknown kind. The entry's InodeKind reports the kind itself.
func (p *PathEntry) GetUnknown() (*PathEntryUnknown, error) {
	if p == nil || p.Dir != nil || p.File != nil || len(p.rawJSON) == 0 {
		return nil, nil
	}
	unknown := new(PathEntryUnknown)
	if err := json.Unmarshal(p.rawJSON, unknown); err != nil {
		return nil, err
	}
	return unknown, nil
}
