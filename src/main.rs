use std::env;
use std::path::Path;
use std::ffi::OsStr;
use std::collections::btree_map;
use libc::{timespec, c_int};
use libc::{EBADF, EPERM, EACCES, S_ISGID, ENOENT, ENOSYS, EINVAL, EEXIST};
use libc::{W_OK, R_OK, X_OK, O_RDONLY, O_WRONLY, O_RDWR, O_TRUNC, O_ACCMODE};
use std::time::{SystemTime, Duration, UNIX_EPOCH};
use fuser::{TimeOrNow, FileAttr, FileType, Filesystem, Request, ReplyAttr, ReplyData, ReplyEntry, ReplyDirectory, ReplyEmpty, ReplyOpen, ReplyStatfs, ReplyWrite, ReplyCreate, MountOption};
use fuser::consts::FOPEN_DIRECT_IO;
use std::collections::BTreeMap;

struct JsonFilesystem {
    tree: BTreeMap<String, String>,
    attrs: BTreeMap<u64, FileAttr>,
    inodes: BTreeMap<String, u64>,
    cur_inode: u64,
    block_size: u32,
    file_handles: BTreeMap<u64, u64>,
}

struct FileInode {
    inode_num: u64,
    attrs: FileAttr,
    path: String,
    data: String, //Should be base64 encoded
    num_links: u32,
}

struct DirectoryInode {
    inode_num: u64,
    attrs: FileAttr,
    path: String,
    contents: Vec<u64>, //List of inode numbers of contents
    num_links: u32,
}

enum Inode {
    FileInode,
    DirectoryInode
}

struct TreeFilesystem {
    tree: BTreeMap<u64, Inode>, //tree[inode_num] = inode
    cur_inode: u64,
    block_size: u32,
    file_handles: BTreeMap<u64, u64>,
}

const FILE_HANDLE_READ_BIT: u64 = 1 << 63;
const FILE_HANDLE_WRITE_BIT: u64 = 1 << 62;

const FMODE_EXEC: i32 = 0x20;

impl TreeFilesystem {
    fn new(contents: &BTreeMap<String, String>) -> TreeFilesystem {
        let mut tree = BTreeMap::new();
        let mut file_handles = BTreeMap::new();
        let mut fs = TreeFilesystem{
            tree: tree,
            cur_inode: 0,
            block_size: 4096,
            file_handles: file_handles,
        };

        fs.create_inode("/".to_string(), FileType::Directory, 0o755, 0, 1000, 1000);

        for (name, data) in contents {
            fs.create(key.clone(), FileType::RegularFile, 0o644, val.to_string().len() as u64, 1000, 1000);
        }
        fs
    }
    
    fn create_inode(&mut self, path: String, ino_type: FileType, mode: u16, size: u64, uid: u32, gid: u32) {
        let curtime = SystemTime::now();
        self.cur_inode += 1;
        let attr = FileAttr{
            ino: self.cur_inode,
            size: size,
            blocks: (size + self.block_size as u64 - 1) / self.block_size as u64,
            atime: curtime,
            mtime: curtime,
            ctime: curtime,
            crtime: curtime,
            kind: ino_type,
            perm: mode,
            nlink: 1,
            uid: uid,
            gid: gid,
            rdev: 0,
            flags: 0,
            blksize: self.block_size,
        };

        let inode = match ino_type {
            FileType::RegularFile => 
                FileInode{
                    inode_num: self.cur_inode,
                    attrs: attr,
                    path: path.clone(),
                    data: "".to_string(),
                    num_links: attr.nlink,
                },
            FileType::Directory =>
                DirectoryInode{
                    inode_num: self.cur_inode,
                    attrs: attr,
                    path: path.clone(),
                    contents: Vec::new(),
                    num_links: attr.nlink,
                }
        };
        self.set_inode(self.cur_inode, inode);
    }

    fn remove_inode(&mut self, ino: u64) {
        println!("remove_inode(ino={}, path={})",ino);
        self.tree.remove(&ino);
    }

    fn set_inode(&mut self, ino: u64, inode_data: Inode) {
        self.tree.insert(ino, inode_data);
    }

    fn get_inode(&self, ino: u64) -> Inode {
        self.tree.get(&ino)
    }

    fn get_inode_by_path(&self, path: String) -> Inode {
        for (_ino_num, ino_data) in &self.tree {
            if path == *ino_data.path {
                return ino_data;
            }
        }
        return 0;
    }

    fn allocate_file_handle(&mut self, ino: u64, can_read: bool, can_write: bool) -> u64 {
        let mut fh_num: u64 = 0;
        if let Some(curfh) = self.file_handles.get(&ino) {
            fh_num = curfh + 1;
        } else {
            fh_num = 1;
        }
        println!("allocate_file_handle: ino={}, fh_num={}", ino, fh_num);


        let mut fh = fh_num;
        // panic if we have run out of file handles
        assert!(fh < FILE_HANDLE_READ_BIT.min(FILE_HANDLE_WRITE_BIT));

        self.file_handles.insert(ino, fh_num);

        if can_read {
            fh |= FILE_HANDLE_READ_BIT;
        }
        if can_write {
            fh |= FILE_HANDLE_WRITE_BIT;
        }
        fh
    }

    fn release_file_handle(&mut self, ino: u64) {
        let fh_num = self.file_handles.get(&ino).unwrap();
        println!("release_file_handle: ino={}, fh_num={}", &ino, fh_num - 1);
        if fh_num.clone() != 0 {
            self.file_handles.insert(ino, fh_num - 1);
        }
    }

    fn get_inode_num_by_path(&self, path: String) -> u64 {
        for (ino_num, ino_data) in &self.tree {
            if path == *ino_data.path {
                return ino_num.clone();
            }
        }
        return 0;
    }

    fn get_path_by_inode_num(&self, ino: u64) -> String {
        for (ino_num, ino_data) in &self.tree {
            if *ino_num == ino {
                return ino_data.path;
            }
        }
        return "".to_string();
    }

    fn get_tree(&self) -> &BTreeMap<String, String> {
        &self.tree
    }

    fn iter_inodes(&self) -> btree_map::Iter<'_, u64, Inode> {
        self.tree.iter()
    }

    fn can_read(&self, mode: u16, uid: u32, gid: u32, req_uid: u32, req_gid: u32) -> bool {
        let is_owner = req_uid == uid;
        let is_in_grp = req_gid == gid;

        // Check octal permissions
        let can_owner_read = mode & 0o400 != 0;
        let can_grp_read = mode & 0o040 != 0;
        let can_other_read = mode & 0o004 != 0;

        if (can_owner_read && is_owner) || (can_grp_read && is_in_grp) || can_other_read || (req_uid == 0 && req_gid == 0) {
            return true;
        }
        false
    }

    fn can_write(&self, mode: u16, uid: u32, gid: u32, req_uid: u32, req_gid: u32) -> bool {
        let is_owner = req_uid == uid;
        let is_in_grp = req_gid == gid;

        // Check octal permissions
        let can_owner_write = mode & 0o200 != 0;
        let can_grp_write = mode & 0o020 != 0;
        let can_other_write = mode & 0o002 != 0;

        if (can_owner_write && is_owner) || (can_grp_write && is_in_grp) || can_other_write || (req_uid == 0 && req_gid == 0) {
            return true;
        }
        false
    }

    fn can_execute(&self, mode: u16, uid: u32, gid: u32, req_uid: u32, req_gid: u32) -> bool {
        let is_owner = req_uid == uid;
        let is_in_grp = req_gid == gid;

        // Check octal permissions
        let can_owner_exec = mode & 0o100 != 0;
        let can_grp_exec = mode & 0o010 != 0;
        let can_other_exec = mode & 0o001 != 0;

        if (can_owner_exec && is_owner) || (can_grp_exec && is_in_grp) || can_other_exec || (req_uid == 0 && req_gid == 0) {
            return true;
        }
        false
    }

}

impl JsonFilesystem {
    fn new(tree: &BTreeMap<String, String>) -> JsonFilesystem {
        let mut attrs = BTreeMap::new();
        let mut inodes = BTreeMap::new();
        let mut file_handles = BTreeMap::new();
        let mut fs = JsonFilesystem{ 
            tree: tree.clone(), 
            attrs: attrs, 
            inodes:inodes, 
            cur_inode: 0,
            block_size: 4096,
            file_handles: file_handles,
        };

        fs.create_inode_no_tree_update("/".to_string(), FileType::Directory, 0o755, 0, 1000, 1000);

        let mut inode: u64 = 0;
        for (key, val) in tree {
            fs.create_inode_no_tree_update(key.clone(), FileType::RegularFile, 0o644, val.to_string().len() as u64, 1000, 1000);
        }
        fs
    }
    
    fn create_inode(&mut self, path: String, ino_type: FileType, mode: u16, size: u64, uid: u32, gid: u32) {
        self.create_inode_no_tree_update(path.clone(), ino_type.clone(), mode.clone(), size.clone(), uid.clone(), gid.clone());
        self.tree.insert(path, "".to_string());
    }

    fn create_inode_no_tree_update(&mut self, path: String, ino_type: FileType, mode: u16, size: u64, uid: u32, gid: u32) {
        let curtime = SystemTime::now();
        self.cur_inode += 1;
        let attr = FileAttr{
            ino: self.cur_inode,
            size: size,
            //blocks: (size % self.block_size as u64),
            blocks: (size + self.block_size as u64 - 1) / self.block_size as u64,
            atime: curtime,
            mtime: curtime,
            ctime: curtime,
            crtime: curtime,
            kind: ino_type,
            perm: mode,
            nlink: 0,
            uid: uid,
            gid: gid,
            rdev: 0,
            flags: 0,
            blksize: self.block_size,
        };
        self.attrs.insert(attr.ino, attr);
        self.inodes.insert(path.to_string(), self.cur_inode);
    }

    fn remove_inode(&mut self, ino: u64, path: String) {
        println!("Remove ino={}, path={}",ino, path);
        self.attrs.remove(&ino);
        self.inodes.remove(&path);
        self.tree.remove(&path);
    }

    fn set_inode(&mut self, ino: u64, path: String) {
        self.inodes.insert(path.to_string(), ino);
    }

    fn allocate_file_handle(&mut self, ino: u64, can_read: bool, can_write: bool) -> u64 {
        let mut fh_num: u64 = 0;
        if let Some(curfh) = self.file_handles.get(&ino) {
            fh_num = curfh + 1;
        } else {
            fh_num = 1;
        }
        println!("allocate_file_handle: ino={}, fh_num={}", ino, fh_num);


        let mut fh = fh_num;
        // Assert that we haven't run out of file handles
        assert!(fh < FILE_HANDLE_READ_BIT.min(FILE_HANDLE_WRITE_BIT));

        self.file_handles.insert(ino, fh_num);

        if can_read {
            fh |= FILE_HANDLE_READ_BIT;
        }
        if can_write {
            fh |= FILE_HANDLE_WRITE_BIT;
        }
        fh
    }

    fn release_file_handle(&mut self, ino: u64) {
        let fh_num = self.file_handles.get(&ino).unwrap();
        println!("release_file_handle: ino={}, fh_num={}", &ino, fh_num - 1);
        if fh_num.clone() != 0 {
            self.file_handles.insert(ino, fh_num - 1);
        }
    }

    fn get_inode_by_path(&self, path: String) -> u64 {
        for (p, inode) in &self.inodes {
            if path == *p {
                return inode.clone();
            }
        }
        return 0;
    }

    fn get_path_by_inode(&self, ino: u64) -> String {
        for (path, inode) in &self.inodes {
            if *inode == ino {
                return path.to_string();
            }
        }
        return "".to_string();
    }

    fn get_tree(&self) -> &BTreeMap<String, String> {
        &self.tree
    }

    fn get_tree_entry(&self, path: String) -> Option<&String> {
        self.tree.get(&path)
    }

    fn remove_tree_entry(&mut self, path: String) {
        self.tree.remove(&path);
    }

    fn set_tree_entry(&mut self, path: String, data: String) {
        self.tree.insert(path, data);
    }

    fn iter_inodes(&self) -> btree_map::Iter<'_, String, u64> {
        self.inodes.iter()
    }

    fn iter_attrs(&self) -> btree_map::Iter<'_, u64, FileAttr> {
        self.attrs.iter()
    }

    fn get_inode(&self, path: String) -> Option<&u64> {
        self.inodes.get(&path)
    }

    fn get_attrs(&self, ino: u64) -> Option<&FileAttr> {
        self.attrs.get(&ino)
    }

    fn set_attrs(&mut self, ino: u64, attr: FileAttr) {
        self.attrs.insert(ino, attr);
    }

    fn can_read(&self, mode: u16, uid: u32, gid: u32, req_uid: u32, req_gid: u32) -> bool {
        let is_owner = req_uid == uid;
        let is_in_grp = req_gid == gid;

        // Check octal permissions
        let can_owner_read = mode & 0o400 != 0;
        let can_grp_read = mode & 0o040 != 0;
        let can_other_read = mode & 0o004 != 0;

        if (can_owner_read && is_owner) || (can_grp_read && is_in_grp) || can_other_read || (req_uid == 0 && req_gid == 0) {
            return true;
        }
        false
    }

    fn can_write(&self, mode: u16, uid: u32, gid: u32, req_uid: u32, req_gid: u32) -> bool {
        let is_owner = req_uid == uid;
        let is_in_grp = req_gid == gid;

        // Check octal permissions
        let can_owner_write = mode & 0o200 != 0;
        let can_grp_write = mode & 0o020 != 0;
        let can_other_write = mode & 0o002 != 0;

        if (can_owner_write && is_owner) || (can_grp_write && is_in_grp) || can_other_write || (req_uid == 0 && req_gid == 0) {
            return true;
        }
        false
    }

    fn can_execute(&self, mode: u16, uid: u32, gid: u32, req_uid: u32, req_gid: u32) -> bool {
        let is_owner = req_uid == uid;
        let is_in_grp = req_gid == gid;

        // Check octal permissions
        let can_owner_exec = mode & 0o100 != 0;
        let can_grp_exec = mode & 0o010 != 0;
        let can_other_exec = mode & 0o001 != 0;

        if (can_owner_exec && is_owner) || (can_grp_exec && is_in_grp) || can_other_exec || (req_uid == 0 && req_gid == 0) {
            return true;
        }
        false
    }
}

impl Filesystem for JsonFilesystem {
    fn getattr(&mut self, __req: &Request, ino: u64, reply: ReplyAttr) {
        println!("getattr(ino={})", ino);
        if let Some(attr) = self.get_attrs(ino) {
                let ttl = Duration::from_secs(1);
                reply.attr(&ttl, attr);
        } else {
            reply.error(ENOENT);
        }
    }

    fn readdir(&mut self, __req: &Request, ino: u64, fh: u64, offset: i64, mut reply: ReplyDirectory) {
        println!("readdir(ino={}, fh={}, offset={})", ino, fh, offset);
        // TODO: Add permissions checks to readdir. 
        // Must have execute on dir for either owner (and be owner), group (and be in group), or
        // other 
        if ino == 1 {
            if offset == 0 {
                let _ = reply.add(1, 0, FileType::Directory, &Path::new("."));
                let _ = reply.add(1, 1, FileType::Directory, &Path::new(".."));
                let mut count = 0;
                for (key, _val) in self.get_tree() {
                    let inode: u64 = 2+count as u64;
                    let offset: i64 = 2+count;
                    println!("\tkey={}, inode={}, offset={}", key, inode, offset);
                    let _ = reply.add(inode, offset, FileType::RegularFile, &Path::new(key.as_str()));
                    count += 1;
                }
            }
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    fn lookup(&mut self, __req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        println!("lookup(parent={}, name={})", parent, name.to_string_lossy());
        if let Some(inode) = self.get_inode(String::from(name.to_str().unwrap())) {
            if let Some(attr) = self.get_attrs(*inode) {
                let ttl = Duration::from_secs(1);
                reply.entry(&ttl, attr, 0);
            } else {
                reply.error(ENOENT);
            }
        } else {
            reply.error(ENOENT);
            return;
        }
    }

    fn read(&mut self, req: &Request, ino: u64, fh: u64, offset: i64, size: u32, flags: i32, _lock: Option<u64>, reply: ReplyData) {
        println!("read(ino={}, fh={}, offset={}, size={}, flags={})", ino, fh, offset, size, flags);
        let attr = self.get_attrs(ino).unwrap();

        if self.can_read(attr.perm, attr.uid, attr.gid, req.uid(), req.gid()) {
            for (key, &inode) in self.iter_inodes() {
                if inode == ino {
                    if let Some(value) = self.get_tree_entry(key.to_string()) {
                        reply.data(value.to_string().as_bytes());
                        return;
                    } else {
                        println!("Cant find tree entry");
                        reply.error(EPERM);
                        return;
                    }
                }
            }
        } else {
            reply.error(EACCES);
            println!("Can't read")
        }
    }

    fn open(&mut self, req: &Request, inode: u64, flags: i32, reply: ReplyOpen) {
        let acc = flags & O_ACCMODE;
        let mut write_allowed = false;
        let mut read_allowed = false;
        let mut exec_allowed = false;
        let mut mode: c_int = 0;

        if acc == O_RDONLY {
            read_allowed = true;
            mode = R_OK;
            // This is undefined behavior; so we bail
            if flags & libc::O_TRUNC != 0 {
                println!("O_TRUNC");
                reply.error(EACCES);
                return;
            }
            if flags & FMODE_EXEC != 0{
                mode = X_OK;
            }
        } else if acc == O_WRONLY {
            write_allowed = true;
            mode = W_OK;
        } else if acc == O_RDWR {
            read_allowed = true;
            write_allowed = true;
            mode = R_OK | W_OK;
        } else {
            println!("Weird flags found in open");
            reply.error(EINVAL);
            return;
        }

        println!("open(inode={}, flags={}, mode={}, acc={})", inode, flags, mode, acc);


        if let Some(attr) = self.get_attrs(inode) {
            let mut perms_match = false;
            match mode {
                R_OK => perms_match = self.can_read(attr.perm, attr.uid, attr.gid, req.uid(), req.gid()),
                W_OK => perms_match = self.can_write(attr.perm, attr.uid, attr.gid, req.uid(), req.gid()),
                X_OK => perms_match = self.can_execute(attr.perm, attr.uid, attr.gid, req.uid(), req.gid()),
                R_OK | W_OK => perms_match = self.can_write(attr.perm, attr.uid, attr.gid, req.uid(), req.gid()) && self.can_read(attr.perm, attr.uid, attr.gid, req.uid(), req.gid()),
                _ => {
                    reply.error(EACCES);
                    return;
                }
            }
            
            if perms_match {
                // Using FOPEN_DIRECT_IO seems to cause an infinite loop when catting a file. Its
                // weird
                //reply.opened(self.allocate_file_handle(inode, read_allowed, write_allowed), FOPEN_DIRECT_IO);
                reply.opened(self.allocate_file_handle(inode, read_allowed, write_allowed), 0);
                return;
            }
        }
        reply.error(ENOSYS);
    }

    fn write(&mut self, _req: &Request, inode: u64, fh: u64, offset: i64, data: &[u8], _write_flags: u32,flags: i32, _lock_owner: Option<u64>, reply: ReplyWrite) {
        println!("write(inode={}, fh={}, offset={}, len(data)={}, flags={})", inode, fh, offset, data.len(), flags);
        // Check if we can write:
        if (fh & FILE_HANDLE_WRITE_BIT) == 0 {
            reply.error(EACCES);
            return;
        }

        let path = self.get_path_by_inode(inode);

        if let Some(current_data) = self.get_tree_entry(path.to_string()) {
            let cur_data = current_data.to_string();
            let mut new_data = Vec::new();

            for i in 0..(offset) {
                new_data.push(cur_data.as_bytes()[i as usize].clone());
            }

            for byte in data.iter() {
                new_data.push(byte.clone());
            }

            for i in offset+1..(cur_data.len() as i64 - 1) {
                new_data.push(cur_data.as_bytes()[i as usize].clone());
            }

            let new_length = new_data.len();
            self.set_tree_entry(path.to_string(), String::from_utf8(new_data).expect("Our bytes should be valid utf8"));
            let attrs = self.get_attrs(inode);
            if let Some(attr) = attrs {
                let mut mattr = attr.clone();
                let now = SystemTime::now();
                mattr.mtime = now;
                mattr.atime = now;
                mattr.size = new_length as u64;
                mattr.blocks = (mattr.size + self.block_size as u64 - 1) / self.block_size as u64;
                self.set_attrs(inode, mattr.clone());
                reply.written(data.len() as u32);
                return;
            } else {
                println!("Could not get attr by inode");
                reply.error(EBADF);
                return;
            }
        } else {
            println!("Could not get current data by path");
            reply.error(EBADF);
            return;
        }
    }

    fn release(&mut self, _req: &Request<'_>, inode: u64, fh: u64, flags: i32, _lock_owner: Option<u64>, flush: bool, reply: ReplyEmpty) {
        println!("release(inode={}, fh={}, flags={}, flush={})", inode, fh, flags, flush);
        self.release_file_handle(inode);
        reply.ok();
    }

    fn unlink(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        println!("unlink(parent={}, name={:?})", parent, name);
        if let Some(attr) = self.get_attrs(parent) {
            if let perms = self.can_write(attr.perm, attr.uid, attr.gid, req.uid(), req.gid()) {
                // Update the metadata for the parent
                let mut mattr = attr.clone();
                let now = SystemTime::now();
                mattr.mtime = now;
                mattr.atime = now;
                self.set_attrs(parent, mattr.clone());

                // Infer the path for the target
                //let parent_path = self.get_path_by_inode(parent);
                //let target_path = Path::new(&parent_path).join(name.to_str().expect("")).into_os_string().into_string().unwrap();
                let target_path = String::from(name.to_str().unwrap());
                let target_ino = self.get_inode_by_path(target_path.clone());
                // Remove the target inode
                self.remove_inode(target_ino, target_path);
                reply.ok();
                return;
            } else {
                reply.error(EACCES);
                return;
            }
        } else {
            reply.error(EBADF);
            return;
        }
        reply.error(ENOSYS);
    }

    fn create(&mut self, req: &Request, parent: u64, name: &OsStr, mut mode: u32, umask: u32, flags: i32, reply: ReplyCreate) {
        println!("create(parent={}, name={}, mode={}, umask={} flags={})", parent, name.to_str().unwrap(), mode, umask, flags);
        //TODO: Add multi-level path support
        //let parent_path = self.get_path_by_inode(parent);
        //let target_path = Path::new(parent_path).join(name.to_str().unwrap())
        //    .into_os_string()
        //    .into_string()
        //    .unwrap();
        let target_path = String::from(name.to_str().unwrap());
        // Check if path exists
        if self.get_inode_by_path(target_path.clone()) != 0 {
            reply.error(EEXIST);
            return;
        }

        // Check flags for read/write (idk why yet)
        let (read, write) = match flags & O_ACCMODE {
            O_RDONLY => (true, false),
            O_WRONLY => (false, true),
            O_RDWR => (true, true),
            _ => {
                reply.error(EINVAL);
                return;
            }
        };

        // Update parent mtime and atime
        if let Some(attr) = self.get_attrs(parent) {
            let now = SystemTime::now();
            let mut mattr = attr.clone();
            mattr.mtime = now;
            mattr.atime = now;

            if let perms = self.can_write(attr.perm, attr.uid, attr.gid, req.uid(), req.gid()) {
                self.create_inode(target_path.clone(), FileType::RegularFile, mode.try_into().unwrap(), 0, req.uid(), req.gid());

            } else {
                reply.error(EACCES);
                return;
            }
            self.set_attrs(parent, mattr.clone());
            let created_ino = self.get_inode_by_path(target_path.clone());
            let fh = self.allocate_file_handle(created_ino.clone(), read, write);
            let created_attrs = self.get_attrs(created_ino.clone()).unwrap();

            reply.created(
                &Duration::new(0,0), 
                &created_attrs, 
                0, 
                fh.clone(), 
                0,
            );
            return;

        } else {
            reply.error(EINVAL);
            return;
        }
        reply.error(ENOSYS);
    }

    fn rename(&mut self, req: &Request, parent: u64, name: &OsStr, new_parent: u64, new_name: &OsStr, flags: u32, reply: ReplyEmpty) {
        println!("rename(parent={}, name={}, new_parent={}, new_name={}, flags={})", parent, name.to_str().unwrap(), new_parent, new_name.to_str().unwrap(), flags);
        //check can_read 'name's inode
        // check can_write new_parent
        //let parent_path = self.get_path_by_inode(parent);
        //let target_path = Path::new(parent_path).join(name.to_str().unwrap())
        //    .into_os_string()
        //    .into_string()
        //    .unwrap();
        //let source_path = Path::new(parent_path).join(name.to_str().unwrap())
        //    .into_os_string()
        //    .into_string()
        //    .unwrap();
        let source_path = String::from(name.to_str().unwrap());
        let target_path = String::from(new_name.to_str().unwrap());

        let source_ino = self.get_inode_by_path(source_path.to_string());

        // Make sure target path doesn't exist
        if self.get_inode_by_path(target_path.to_string()) != 0 {
            reply.error(EINVAL);
            return;
        }

        // Get all attrs
        let mut source_attr = match self.get_attrs(source_ino) {
            Some(a) => a.clone(),
            _ => {
                reply.error(EPERM);
                return;
            },
        };

        let mut parent_attr = match self.get_attrs(parent) {
            Some(a) => a.clone(),
            _ => {
                reply.error(EPERM);
                return;
            },
        };

        let mut new_parent_attr = match self.get_attrs(new_parent) {
            Some(a) => a.clone(),
            _ => {
                reply.error(EPERM);
                return;
            },
        };

        // Check that we can read the source, and write to the new parent
        if !self.can_read(source_attr.perm, source_attr.uid, source_attr.gid, req.uid(), req.gid()) ||
            !self.can_write(new_parent_attr.perm, new_parent_attr.uid, new_parent_attr.gid, req.uid(), req.gid()) {
            reply.error(EPERM);
            return;
        }

        //update the mtime/atime of the old and new parents
        let now = SystemTime::now();
        source_attr.mtime = now;
        source_attr.atime = now;
        parent_attr.mtime = now;
        parent_attr.atime = now;
        new_parent_attr.mtime = now;
        new_parent_attr.atime = now;

        //Update the parents
        self.set_attrs(parent, parent_attr);
        self.set_attrs(new_parent, new_parent_attr);

        // Set the attrs for the new inode
        // Copy the data and remove the old entry
        let data = self.get_tree_entry(source_path.to_string()).unwrap().clone();
        // Remove the old tree entry; file data is now completely in memory
        self.remove_inode(source_ino, source_path);
        // Set the new attrs
        self.set_attrs(source_ino.clone(), source_attr);
        // Write the new data to the path
        self.set_tree_entry(target_path.to_string(), data);
        //Set the inode to point to the new path
        self.set_inode(source_ino, target_path);
        //self.remove_inode(source_ino, source_path);

        reply.ok();

    }

    fn link(&mut self, _req: &Request, inode: u64, new_parent: u64, new_name: &OsStr, reply: ReplyEntry) {
        println!("link(inode={}, new_parent={}, new_name={})", inode, new_parent, new_name.to_str().unwrap());
        reply.error(ENOSYS);
    }

    //fn symlink(&mut self, _req: &Request, parent: u64, link_name: &OsStr, target: &Path, reply: ReplyEntry) {
    //    println!("symlink(parent={}, link_name={}, target={})", parent, link_name, target);
    //    reply.error(ENOSYS);
    //}

    //fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
    //    println!("rmdir(parent={}, name={})", parent, name);
    //    reply.error(ENOSYS);
    //}

    //fn mkdir(&mut self, _req: &Request, parent: u64, name: &OsStr, mut mode: u32, _umask: u32, reply: ReplyEntry) {
    //    println!("mkdir(parent={}, name={}, mode={})", parent, name, mode);
    //    reply.error(ENOSYS);
    //}

    //fn access(&mut self, _req: &Request, inode: u64, mask: i32, reply: ReplyEmpty) {
    //    println!("access(inode={}, mask={})", inode, mask);
    //    reply.error(ENOSYS);
    //}

    //fn opendir(&mut self, __req: &Request, inode: u64, flags: i32, reply: ReplyOpen) {
    //    println!("opendir(inode={}, flags={})", inode, flags);
    //    reply.error(ENOSYS);
    //}

    //fn releasedir(&mut self, __req: &Request<'_>, inode: u64, _fh: u64, _flags: i32, reply: ReplyEmpty) {
    //    println!("releasedir(inode={})", inode);
    //    reply.error(ENOSYS);
    //}

    //fn statfs(&mut self, __req: &Request, ino: u64, reply: ReplyStatfs) {
    //    println!("statfs(inode={})", ino);
    //    reply.error(ENOSYS);
    //}

    //fn readlink(&mut self, __req: &Request, inode: u64, reply: ReplyData) {
    //   println!("mkdir(inode={})", inode);
    //   reply.error(ENOSYS);
    //}
    
    //fn fallocate(&mut self, __req: &Request<'_>, inode: u64, _fh: u64, offset: i64, length: i64, mode: i32, reply: ReplyEmpty) {
    //    println!("fallocate(inode={}, offset={}, length={}, mode={})", inode, offset, length, mode);
    //    reply.error(ENOSYS);
    //}



    fn setattr(&mut self, req: &Request, inode: u64, mode: Option<u32>, uid: Option<u32>, gid: Option<u32>, size: Option<u64>, atime: Option<TimeOrNow>, mtime: Option<TimeOrNow>, _ctime: Option<SystemTime>, fh: Option<u64>, _crtime: Option<SystemTime>, _chgtime: Option<SystemTime>, _bkuptime: Option<SystemTime>, _flags: Option<u32>, reply: ReplyAttr) {
       println!("setattr(inode={:?}, mode={:?}, uid={:?}, gid={:?}, size={:?}, atime={:?}, mtime={:?}, fh={:?})", inode, mode, uid, gid, size, atime, mtime, fh);
       let mut attr = self.get_attrs(inode).unwrap().clone();

       // If the req is not from root
       // If it is from root, just let it pass
       if req.uid() != 0 && req.gid() != 0 {
           // if the user is not part of the group and not the owner, bail
           if req.uid() != attr.uid && req.gid() != attr.gid {
               reply.error(EPERM);
               return;
           }
       }
       if let Some(m) = mode {
           attr.perm = (m & !S_ISGID as u32) as u16;
       }
       if let Some(u) = uid {
           attr.uid = u;
       }
       if let Some(g) = gid {
           attr.gid = g;
       }
       if let Some(s) = size {
           attr.size = s;
       }
       if let Some(a) = atime {
           if let TimeOrNow::Now = a {
               attr.atime = SystemTime::now();
           } else if let TimeOrNow::SpecificTime(time) = a {
               attr.atime = time;
           }
       }
       if let Some(t) = mtime {
           if let TimeOrNow::Now = t {
               attr.mtime = SystemTime::now();
           } else if let TimeOrNow::SpecificTime(time) = t {
               attr.mtime = time;
           }
       }

       self.set_attrs(inode, attr.clone());
       reply.attr(&Duration::new(0, 0), &attr);
    }
}

fn main() {
    let mut data = BTreeMap::new();
    data.insert("foo".to_string(), "bar".to_string());
    data.insert("answer".to_string(), "42".to_string());
    let fs = JsonFilesystem::new(&data);
    let mountpoint = match env::args().nth(1) {
        Some(path) => path,
        None => {
            println!("Usage: {} <MOUNTPOINT>", env::args().nth(0).unwrap());
            return;
        }
    };

    let mut options = Vec::new();
    options.push(MountOption::FSName("jakefs".to_string()));
    options.push(MountOption::AutoUnmount);
    //options.push(MountOption::Suid);
    options.push(MountOption::AllowOther);

    let ret = fuser::mount2(fs, &mountpoint, &options);
    if let Err(e) = ret {
        if e.kind() == std::io::ErrorKind::PermissionDenied {
            println!("Permission Denied: add 'user_allow_other' in fuse.conf");
            std::process::exit(1);
        }
    }
}
