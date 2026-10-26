//! The kernel's requests. Each request either calls the matching
//! `MountFilesystem` method or is refused with the error that the design
//! note's table lists.

use super::MountFilesystem;
use crate::attributes::GENERATION;
use crate::errors::Refusal;
use fuser::{
    BsdFileFlags, CopyFileRangeFlags, Errno, FileHandle, Filesystem, INodeNo, InitFlags,
    IoctlFlags, KernelConfig, LockOwner, OpenFlags, PollEvents, PollFlags, PollNotifier,
    RenameFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty,
    ReplyEntry, ReplyIoctl, ReplyLseek, ReplyOpen, ReplyPoll, ReplyStatfs, ReplyWrite, ReplyXattr,
    Request, TimeOrNow, WriteFlags,
};
use loonfs_mount::ItemId;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt as _;
use std::path::Path;
use std::time::SystemTime;

impl Filesystem for MountFilesystem {
    fn init(&mut self, _request: &Request, config: &mut KernelConfig) -> std::io::Result<()> {
        // `fuser` leaves readdirplus off, and without it every `ls -l` sends
        // a lookup for each entry.
        if let Err(unsupported) = config.add_capabilities(InitFlags::FUSE_DO_READDIRPLUS) {
            tracing::warn!(?unsupported, "the kernel doesn't support readdirplus");
        }
        Ok(())
    }

    fn lookup(&self, _request: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        match self.lookup_item(ItemId(parent.0), name) {
            Some((attr, lifetime)) => reply.entry(&lifetime, &attr, GENERATION),
            None => reply.error(Errno::ENOENT),
        }
    }

    fn forget(&self, _request: &Request, item: INodeNo, lookups: u64) {
        self.forget_item(ItemId(item.0), lookups);
    }

    fn getattr(
        &self,
        _request: &Request,
        item: INodeNo,
        _handle: Option<FileHandle>,
        reply: ReplyAttr,
    ) {
        match self.attributes(ItemId(item.0)) {
            Some((attr, lifetime)) => reply.attr(&lifetime, &attr),
            None => reply.error(Errno::ENOENT),
        }
    }

    fn setattr(
        &self,
        _request: &Request,
        _item: INodeNo,
        _mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _handle: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        reply.error(Refusal::of_setattr(uid, gid).errno());
    }

    fn readlink(&self, _request: &Request, item: INodeNo, reply: ReplyData) {
        match self.engine.readlink(ItemId(item.0)) {
            Some(target) => reply.data(target.as_bytes()),
            None => reply.error(Errno::EINVAL),
        }
    }

    fn mknod(
        &self,
        _request: &Request,
        _parent: INodeNo,
        _name: &OsStr,
        mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        reply.error(Refusal::of_mknod(mode).errno());
    }

    fn mkdir(
        &self,
        _request: &Request,
        _parent: INodeNo,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        reply.error(Refusal::Change.errno());
    }

    fn unlink(&self, _request: &Request, _parent: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(Refusal::Change.errno());
    }

    fn rmdir(&self, _request: &Request, _parent: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(Refusal::Change.errno());
    }

    fn symlink(
        &self,
        _request: &Request,
        _parent: INodeNo,
        _name: &OsStr,
        _target: &Path,
        reply: ReplyEntry,
    ) {
        reply.error(Refusal::Change.errno());
    }

    fn rename(
        &self,
        _request: &Request,
        _parent: INodeNo,
        _name: &OsStr,
        _new_parent: INodeNo,
        _new_name: &OsStr,
        _flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        reply.error(Refusal::Change.errno());
    }

    fn link(
        &self,
        _request: &Request,
        _item: INodeNo,
        _new_parent: INodeNo,
        _new_name: &OsStr,
        reply: ReplyEntry,
    ) {
        reply.error(Refusal::HardLink.errno());
    }

    fn open(&self, _request: &Request, item: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        self.open_file(ItemId(item.0), flags, reply);
    }

    fn read(
        &self,
        _request: &Request,
        _item: INodeNo,
        handle: FileHandle,
        offset: u64,
        length: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        self.read_file(handle, offset, length, reply);
    }

    fn write(
        &self,
        _request: &Request,
        _item: INodeNo,
        _handle: FileHandle,
        _offset: u64,
        _data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        reply.error(Refusal::Change.errno());
    }

    /// Returns at once: the kernel sends a flush for each close of each
    /// duplicated descriptor, and nothing is buffered here.
    fn flush(
        &self,
        _request: &Request,
        _item: INodeNo,
        _handle: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn release(
        &self,
        _request: &Request,
        _item: INodeNo,
        handle: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        self.open_files.remove(handle);
        reply.ok();
    }

    fn fsync(
        &self,
        _request: &Request,
        _item: INodeNo,
        _handle: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        self.sync(reply);
    }

    fn fsyncdir(
        &self,
        _request: &Request,
        _item: INodeNo,
        _handle: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        self.sync(reply);
    }

    /// Lets the kernel handle `poll` itself, which reports a file as always
    /// ready, as a local file is. This method replies with `ENOSYS` itself,
    /// because `fuser`'s default implementation logs a warning on every
    /// call.
    fn poll(
        &self,
        _request: &Request,
        _item: INodeNo,
        _handle: FileHandle,
        _notifier: PollNotifier,
        _events: PollEvents,
        _flags: PollFlags,
        reply: ReplyPoll,
    ) {
        reply.error(Errno::ENOSYS);
    }

    /// Lets the kernel handle `lseek` itself, which finds no holes in a
    /// file. This method replies with `ENOSYS` itself, because `fuser`'s
    /// default implementation logs a warning on every call.
    fn lseek(
        &self,
        _request: &Request,
        _item: INodeNo,
        _handle: FileHandle,
        _offset: i64,
        _whence: i32,
        reply: ReplyLseek,
    ) {
        reply.error(Errno::ENOSYS);
    }

    fn readdir(
        &self,
        _request: &Request,
        directory: INodeNo,
        _handle: FileHandle,
        offset: u64,
        reply: ReplyDirectory,
    ) {
        self.read_directory(ItemId(directory.0), offset, reply);
    }

    fn readdirplus(
        &self,
        _request: &Request,
        directory: INodeNo,
        _handle: FileHandle,
        offset: u64,
        reply: ReplyDirectoryPlus,
    ) {
        self.read_directory_plus(ItemId(directory.0), offset, reply);
    }

    fn statfs(&self, _request: &Request, _item: INodeNo, reply: ReplyStatfs) {
        self.report_statfs(reply);
    }

    fn setxattr(
        &self,
        _request: &Request,
        _item: INodeNo,
        _name: &OsStr,
        _value: &[u8],
        _flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        reply.error(Refusal::ExtendedAttribute.errno());
    }

    fn getxattr(
        &self,
        _request: &Request,
        _item: INodeNo,
        _name: &OsStr,
        _size: u32,
        reply: ReplyXattr,
    ) {
        reply.error(Refusal::ExtendedAttribute.errno());
    }

    fn listxattr(&self, _request: &Request, _item: INodeNo, _size: u32, reply: ReplyXattr) {
        reply.error(Refusal::ExtendedAttribute.errno());
    }

    fn removexattr(&self, _request: &Request, _item: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(Refusal::ExtendedAttribute.errno());
    }

    fn create(
        &self,
        _request: &Request,
        _parent: INodeNo,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        reply.error(Refusal::Change.errno());
    }

    fn ioctl(
        &self,
        _request: &Request,
        _item: INodeNo,
        _handle: FileHandle,
        _flags: IoctlFlags,
        _command: u32,
        _in_data: &[u8],
        _out_size: u32,
        reply: ReplyIoctl,
    ) {
        reply.error(Refusal::Ioctl.errno());
    }

    fn fallocate(
        &self,
        _request: &Request,
        _item: INodeNo,
        _handle: FileHandle,
        _offset: u64,
        _length: u64,
        _mode: i32,
        reply: ReplyEmpty,
    ) {
        reply.error(Refusal::Fallocate.errno());
    }

    fn copy_file_range(
        &self,
        _request: &Request,
        _item_in: INodeNo,
        _handle_in: FileHandle,
        _offset_in: u64,
        _item_out: INodeNo,
        _handle_out: FileHandle,
        _offset_out: u64,
        _length: u64,
        _flags: CopyFileRangeFlags,
        reply: ReplyWrite,
    ) {
        reply.error(Refusal::CopyFileRange.errno());
    }
}
