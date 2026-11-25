# Object checksums in references

Every durable reference in LoonFS records a checksum of the object that it refers to. A manifest reference records a digest of the manifest's payload, a segment descriptor records the checksum that the store keeps for the segment object, and an [extent](../specs/glossary.md#extent) in a layout records the checksum that the store keeps for its content object. A content reference records the checksum of the file's bytes.

The storage format defines the fields and the rules:

- [Manifest references](../specs/format.md#25-manifest-references) lists the references and what each one records.
- [Immutable writes and retries](../specs/format.md#32-immutable-writes-and-retries) defines the stored checksum and where each writer takes the value from.
- [Content verification](../specs/format.md#45-content-verification) defines which reads check an object checksum.
- [Control and manifest payloads](../specs/format.md#a4-control-and-manifest-payloads) and [Metadata rows and row keys](../specs/format.md#a6-metadata-rows-and-row-keys) define the descriptor's `checksum` and the extent's `object_checksum`.

This note explains why the references record the store's own checksum, what that allows, and what it costs.

## Why each reference records its object's checksum

A namespace's durable state is a tree of references. A snapshot refers to a manifest, the manifest lists its segments, a layout row in a `content_layouts` segment lists the content objects of a chain, and a file revision refers to a prefix of that chain. If one of those objects were replaced, truncated, or damaged in storage, a reader that followed the reference would find bytes that the reference wasn't written for.

When each reference records a checksum of its object, a reader can check the object it found against the reference it followed. The check needs no other object, so it works for an object that many namespaces share, such as a content object that a fork inherits or a segment that a fork's manifest lists from its source.

## The store's own checksum

Every content checksum in LoonFS is in the store's own algorithm: CRC-64/NVME on S3, R2, and local storage, and CRC-32C on GCS. A writer sends the checksum with each write, the provider verifies it and stores it, and an [assembly](../specs/glossary.md#assembly) combines the checksums of its sources without reading their bytes. LoonFS doesn't keep a digest that the writer computes on its own, such as SHA-256 metadata, because the provider doesn't verify it and an assembly couldn't combine it.

An object checksum follows the same rule. It's the full-object checksum that the store keeps for the object, the same value that a HEAD request returns, so the reference and the store record the same value for each object. The writer of an object already has that value when the write returns:

| Object | Where the recorded value comes from |
| --- | --- |
| A span object that a fold, a validation, or a merge writes | The stored checksum that the assembly returns, which the assembly checked against the combined checksum of its sources and the tail's pieces |
| The empty whole object of a chain of zero bytes | The stored checksum that the assembly returns |
| An upload's whole object | The upload's reference checksum, which covers the whole object, and which the upload's completion compared with the stored checksum |
| A metadata segment | The checksum of the segment's bytes in the store's algorithm, which the provider verifies on the segment's put |

For example, suppose a fold combines a chain's last extent of 100 bytes with 30 bytes of tail pieces. The fold already sends one HEAD request to get the stored checksum of the 100-byte object, combines it with the checksum of the 30 bytes, and passes the result to the assembly as its expected checksum. The new extent records the stored checksum that the assembly returns, and no byte is read or hashed to produce it.

A segment's descriptor is built differently, because the descriptor is needed before the segment's put returns. A fold or a compaction writes each segment while it encodes the next one, and the run lists the segment's descriptor at once. So the writer computes the segment's checksum over the bytes that it has in memory, and the put checks that the provider stores the same value.

Every extent that lists one object records the same object checksum, whatever part of the object the extent takes. For example, a new chain that shares the first 100 bytes of a 130-byte span object records the checksum of all 130 bytes. So a layout row can be checked against the objects it lists without the content references that use the layout row.

## What the checksums allow

A verification job could check every object that a namespace's references reach with one HEAD request per object, without reading any bytes. It compares the stored checksum in each response with the value that the reference records. For example, checking a namespace whose layouts list one million content objects costs one million HEAD requests, however large the objects are, while reading the objects would transfer every byte of them.

A read that already fetches a whole object checks it against the recorded value, with no extra request:

- A ranged GET that returns a complete object (from offset 0 to the object's length), whether an upload's whole object or a [span object](../specs/glossary.md#span-object), is checked against the extent's object checksum. This covers each object that a whole-file read fetches in full, and each object that a [ranged read](../specs/glossary.md#ranged-read) covers completely. An object that an extent takes only part of, such as a shared base extent of 100 bytes in a 130-byte object, isn't checked.
- A segment small enough to fetch whole (up to 256 KiB) is checked against its descriptor's checksum, both when a lookup loads it and when a compaction reads it as an input.

A mismatch is corruption, and the error identifies the object's key, so an operator knows which object to look at, rather than only which file failed its whole-file check.

When LoonFS assembles a new span object from a chain's existing extents (in a fold, in a validation that writes pending pieces, or in a merge's write of a fork's pending pieces), it checks each source object first. The assembly already needs one HEAD request per source object, to get the stored checksum that it combines into the new object's checksum. LoonFS compares that stored checksum with the extent's object checksum, so the check adds no request. A mismatch is corruption, and the write fails instead of assembling a span object from the wrong bytes. (If the write went ahead, the new extent would record a checksum computed from the wrong bytes, and every later check of that span object would pass.)

## What the checksums don't check

A ranged read inside one object isn't checked. The object checksum is a CRC of the whole object, and a CRC of the whole object can't check a slice taken from the middle of it. For example, a ranged read of bytes 110 to 120 of a chain whose layout lists a 30-byte span object that starts at chain offset 100 reads 10 bytes from the middle of that object, and the reader checks only the length of the response.

A segment larger than 256 KiB is read in sections, and each section is already checked against the CRC32C in its block handle.

## Costs

The object checksums have these costs:

- **Bytes.** A segment descriptor's `checksum` adds 64 bytes of JSON to the manifest, and an extent's `object_checksum` adds 60 bytes of CBOR to each layout row and to each revision delta that includes a layout. (A CRC-32C value is 8 bytes shorter.) After zstd compression, a descriptor or an extent costs about 11 bytes more, because the field names and the algorithm repeat and only the 8 bytes of the CRC differ. That figure comes from compressing 1,000 descriptors with and without the field.
- **Memory.** Each extent and each descriptor that a process keeps in memory is 48 bytes larger for a CRC-64/NVME checksum: 32 bytes in the struct and 16 bytes of hexadecimal on the heap. The memory accounting of layouts and segment descriptors includes both.
- **CPU.** A segment's writer computes one more CRC over the segment's bytes, which is small next to the zstd compression of the same bytes. A whole-file read of a file that is one object, fetched in one GET, computes one CRC of its bytes and compares it with both the object checksum and the reference's checksum. Every other whole-file read hashes each complete object that one GET fetches twice, once for the object checksum and once with the rest of the file for the reference's checksum. One extra pass costs well under a millisecond per megabyte (measured in a release build on a development VM, hashing a megabyte took 21 microseconds with CRC-64/NVME and 18 with CRC-32C). Combining the CRCs of the byte ranges that a read fetches would avoid that pass, but in the same measurement one combine took 91 to 235 microseconds with CRC-64/NVME and 11 to 30 with CRC-32C, as long as hashing between half a megabyte and ten megabytes.
- **Requests.** No write or read sends an extra request to produce or check an object checksum.

## Alternatives considered

This section describes other designs for checking the objects that references point to, and why LoonFS doesn't use them.

- **Per-block checksums in a footer of each content object.** Each content object would end with a table of checksums, one for each block of a fixed size such as 64 KiB, so a ranged read could check every block it reads. This is the only design here that checks a range inside one object. But a content object would no longer be the file's raw bytes, so a direct download would have to skip the footer, and an assembly that copies sources inside the provider would have to write a new footer instead of combining the sources' checksums. A ranged read would also need the footer before it could check a block, which costs a second GET or a cache of footers.
- **A HEAD request before each read.** A reader could compare the object's stored checksum with the content reference before it reads the object. This adds one request to every read, and it checks only what the store reports, not the bytes that the read returns.
- **A digest that the writer computes.** A SHA-256 of each object, recorded in the reference, would cover the bytes with a stronger digest. The provider doesn't verify it, every write would hash its bytes a second time, and an assembly couldn't combine the digests of its sources without reading them.
