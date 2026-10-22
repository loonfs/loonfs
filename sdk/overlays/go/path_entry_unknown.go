package loonfs

import "encoding/json"

// PathEntryUnknown contains the fields that every path entry includes, for an entry of an inode
// kind that this version of the SDK doesn't define (such as a kind that a newer server added). A
// directory entry includes exactly those fields, so this type has the fields of
// PathEntryDirectory.
type PathEntryUnknown PathEntryDirectory

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
