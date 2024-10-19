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

const FILE_HANDLE_READ_BIT: u64 = 1 << 63;
const FILE_HANDLE_WRITE_BIT: u64 = 1 << 62;

const FMODE_EXEC: i32 = 0x20;

#[derive(Debug,Clone,PartialEq)]
struct FileInode {
    inode_num: u64,
    attrs: FileAttr,
    path: String,
    data: String, //Should be base64 encoded
    num_links: u32,
    name: String,
    parent: u64,
}

#[derive(Debug,Clone,PartialEq)]
struct DirectoryInode {
    inode_num: u64,
    attrs: FileAttr,
    path: String,
    contents: Vec<u64>, //List of inode numbers of contents
    num_links: u32,
    parent: u64,
    name: String,
}

#[derive(Debug,Clone,PartialEq)]
enum Inode {
    FileInode(FileInode),
    DirectoryInode(DirectoryInode),
}

trait InodeTrait {
    fn inode_num(&self) -> u64;
    fn attrs(&self) -> &FileAttr;
    fn path(&self) -> &String;
    fn data(&self) -> &String;
    fn parent(&self) -> u64;
    fn name(&self) -> &String;
    fn contents(&self) -> &Vec<u64>;
    fn set_attrs(&mut self, _: FileAttr);
    fn set_path(&mut self, _: String);
    fn set_inode_num(&mut self, _: u64);
    fn set_parent(&mut self, _:u64);
    fn set_data(&mut self, _: String);
    fn set_contents(&mut self, _: Vec<u64>);
}

impl InodeTrait for Inode {
    fn inode_num(&self) -> u64 {
        match self {
            Inode::FileInode(ref a) => return a.inode_num.clone(),
            Inode::DirectoryInode(ref b) => return b.inode_num.clone(),
        };
    }
    fn attrs(&self) -> &FileAttr {
        match self {
            Inode::FileInode(ref a) => return &a.attrs,
            Inode::DirectoryInode(ref b) => return &b.attrs,
        };
    }
    fn path(&self) -> &String {
        match self {
            Inode::FileInode(ref a) => return &a.path,
            Inode::DirectoryInode(ref b) => return &b.path,
        };
    }
    fn data(&self) -> &String {
        match self {
            Inode::FileInode(ref a) => return &a.data,
            Inode::DirectoryInode(_) => todo!(),
        };
    }
    fn parent(&self) -> u64 {
        match self {
            Inode::FileInode(ref a) => return a.parent.clone(),
            Inode::DirectoryInode(ref b) => return b.parent.clone(),
        };
    }

    fn name(&self) -> &String {
        match self {
            Inode::FileInode(ref a) => return &a.name,
            Inode::DirectoryInode(ref b) => return &b.name,
        };
    }

    fn contents(&self) -> &Vec<u64> {
        match self {
            Inode::FileInode(_) => todo!(),
            Inode::DirectoryInode(ref b) => return &b.contents,
        }
    }

    fn set_attrs(&mut self, attrs: FileAttr) {
        match self {
            Inode::FileInode(ref mut a) =>  a.attrs = attrs,
            Inode::DirectoryInode(ref mut b) =>  b.attrs = attrs,
        };
    }

    fn set_path(&mut self, path: String) {
        match self {
            Inode::FileInode(ref mut a) =>  a.path = path,
            Inode::DirectoryInode(ref mut b) =>  b.path = path,
        };
    }

    fn set_inode_num(&mut self, ino: u64) {
        match self {
            Inode::FileInode(ref mut a) =>  {
                a.inode_num = ino.clone();
                a.attrs.ino = ino.clone();
            },
            Inode::DirectoryInode(ref mut b) =>  {
                b.inode_num = ino.clone();
                b.attrs.ino = ino.clone();
            },
        };
    }
    fn set_parent(&mut self, parent: u64) {
        match self {
            Inode::FileInode(ref mut a) =>  a.parent = parent,
            Inode::DirectoryInode(ref mut b) =>  b.parent = parent,
        };
    }
    fn set_data(&mut self, data: String) {
        match self {
            Inode::FileInode(ref mut a) =>  a.data = data.to_string(),
            Inode::DirectoryInode(_) => todo!(),
        };
    }
    fn set_contents(&mut self, data: Vec<u64>) {
        match self {
            Inode::FileInode(_) =>  todo!(),
            Inode::DirectoryInode(ref mut a) => a.contents = data.clone(),
        };
    }
}


struct JsonFilesystem {
    tree: BTreeMap<String, String>,
    attrs: BTreeMap<u64, FileAttr>,
    inodes: BTreeMap<String, u64>,
    cur_inode: u64,
    block_size: u32,
    file_handles: BTreeMap<u64, u64>,
}

#[derive(Debug)]
struct TreeFilesystem {
    tree: BTreeMap<u64, Inode>, //tree[inode_num] = inode
    cur_inode: u64,
    block_size: u32,
    file_handles: BTreeMap<u64, u64>,
}

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

        let _ = fs.create_inode("/".to_string(), FileType::Directory, 0o755, 0, 1000, 1000, 0, "".to_string());

        for (name, data) in contents {
            let _ = fs.create_inode(name.clone(), FileType::RegularFile, 0o644, data.to_string().len() as u64, 1000, 1000, 1, data.to_string());
        }
        dbg!(fs.tree.clone());
        fs
    }
    
    fn create_inode(&mut self, path: String, ino_type: FileType, mode: u16, size: u64, uid: u32, gid: u32, parent: u64, data: String) -> &Inode {
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

        let mut name = path.clone();
        if path != "/" {
            name = Path::new(&path).file_name().unwrap().to_str().unwrap().to_string();
        }
        let inode: Inode = match ino_type {
            FileType::RegularFile => 
                Inode::FileInode(FileInode{
                    inode_num: self.cur_inode,
                    attrs: attr,
                    path: path.clone(),
                    data: data.clone(),
                    num_links: attr.nlink,
                    parent: parent,
                    name: name,
                }),
            FileType::Directory =>
                Inode::DirectoryInode(DirectoryInode{
                    inode_num: self.cur_inode,
                    attrs: attr,
                    path: path.clone(),
                    contents: Vec::new(),
                    num_links: attr.nlink,
                    parent: parent,
                    name: name,
                }),
            _ => todo!(),
        };

        // Update the contents of the parent here!
        if self.cur_inode != 1 {
            let mut parent_inode = self.get_inode(parent).unwrap().clone();
            let mut pcontents = parent_inode.contents().clone();
            pcontents.push(self.cur_inode);
            parent_inode.set_contents(pcontents);
            self.set_inode(parent_inode.inode_num(), parent_inode);
        }

        self.set_inode(self.cur_inode, inode);
        self.get_inode(self.cur_inode).unwrap()
    }

    fn remove_inode(&mut self, ino: u64) {
        println!("remove_inode(ino={})",ino);
        self.tree.remove(&ino);
    }

    fn set_inode(&mut self, ino: u64, inode_data: Inode) {
        self.tree.insert(ino, inode_data);
    }

    fn get_inode(&self, ino: u64) -> Option<&Inode> {
        self.tree.get(&ino)
    }

    fn get_inode_by_path(&self, path: String) -> Option<&Inode> {
        for (_ino_num, ino_data) in &self.tree {
            if path == *ino_data.path() {
                return Some(ino_data);
            }
        }
        return None;
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

impl Filesystem for TreeFilesystem {
    fn getattr(&mut self, __req: &Request, ino: u64, reply: ReplyAttr) {
        println!("getattr(ino={})", ino);
        let inode_data = match self.get_inode(ino) {
            Some(a) => match a {
                Inode::FileInode(ref a) => Inode::FileInode(a.clone()),
                Inode::DirectoryInode(ref b) => Inode::DirectoryInode(b.clone()),
            },
            None => {
                reply.error(ENOENT);
                return;
            },
        };
        let ttl = Duration::from_secs(1);
        reply.attr(&ttl, inode_data.attrs());
    }

    fn readdir(&mut self, _req: &Request, ino: u64, fh: u64, offset: i64, mut reply: ReplyDirectory) {
        println!("readdir(ino={}, fh={}, offset={})", ino, fh, offset);
        // TODO: Add permissions checks to readdir. 
        // Must have execute on dir for either owner (and be owner), group (and be in group), or
        // other 
        //TODO: Fix this by inferring . and .. based upon tree
        let mut count = 0;
        let dir_inode = self.get_inode(ino).unwrap();//match self.get_inode(ino).unwrap() {
        //dbg!(dir_inode);
        let dir_contents = dir_inode.contents().clone();
        if offset == 0 {
            let _ = reply.add(dir_inode.inode_num(), 0, FileType::Directory, &Path::new("."));
            let _ = reply.add(dir_inode.inode_num(), 1, FileType::Directory, &Path::new(".."));

            for (idx, cur_ino) in dir_contents.iter().skip(offset as usize).enumerate() {
                let ino_data = match self.get_inode(*cur_ino) {
                    Some(a) => a.clone(),
                    None => todo!(),
                };
                dbg!(ino_data.clone());
                println!("\tkey={}, inode={}, offset={}", ino_data.name(), ino_data.inode_num(), offset);
                let _ = reply.add(ino_data.inode_num(), (idx as i64) + 2, ino_data.attrs().kind, &Path::new(ino_data.name()));
            }
        }
        reply.ok();
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        println!("lookup(parent={}, name={})", parent, name.to_string_lossy());

        let parent_ino = match self.get_inode(parent) {
            Some(a) => match a {
                Inode::FileInode(ref b) => Inode::FileInode(b.clone()),
                Inode::DirectoryInode(ref c) => Inode::DirectoryInode(c.clone()),
            },
            None => {
                reply.error(ENOENT);
                return;
            },
        };

        //TODO Add permissions check here

        for child_ino in parent_ino.contents() {
            let child = match self.get_inode(*child_ino) {
                Some(a) => a.clone(),
                None => continue,
            };

            if *child.name() == name.to_string_lossy() {
                let ttl = Duration::from_secs(1);
                reply.entry(&ttl, child.attrs(), 0);
                return;
            }
        }
        reply.error(ENOENT);
    }

    fn read(&mut self, req: &Request, ino: u64, fh: u64, offset: i64, size: u32, flags: i32, _lock: Option<u64>, reply: ReplyData) {
        println!("read(ino={}, fh={}, offset={}, size={}, flags={})", ino, fh, offset, size, flags);

        let ino_data = match self.get_inode(ino) {
            Some(a) => match a {
		Inode::FileInode(ref a) => Inode::FileInode(a.clone()),
		Inode::DirectoryInode(ref b) => Inode::DirectoryInode(b.clone()),
            },
            None => {
                reply.error(EPERM);
                return;
            },
        };


        if self.can_read(ino_data.attrs().perm, ino_data.attrs().uid, ino_data.attrs().gid, req.uid(), req.gid()) {
            reply.data(ino_data.data().to_string().as_bytes());
        } else {
            reply.error(EACCES);
            println!("Can't read")
        }
    }

    fn open(&mut self, req: &Request, inode: u64, flags: i32, reply: ReplyOpen) {
        println!("Open started");
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

        let ino_data = match self.get_inode(inode) {
            Some(a) => match a {
                Inode::FileInode(ref b) => Inode::FileInode(b.clone()),
                Inode::DirectoryInode(ref c) => Inode::DirectoryInode(c.clone()),
            },
            None => {
                reply.error(ENOSYS);
                return;
            },
        };

        let mut perms_match = false;
        match mode {
            R_OK => perms_match = self.can_read(ino_data.attrs().perm, ino_data.attrs().uid, ino_data.attrs().gid, req.uid(), req.gid()),
            W_OK => perms_match = self.can_write(ino_data.attrs().perm, ino_data.attrs().uid, ino_data.attrs().gid, req.uid(), req.gid()),
            X_OK => perms_match = self.can_execute(ino_data.attrs().perm, ino_data.attrs().uid, ino_data.attrs().gid, req.uid(), req.gid()),
            R_OK | W_OK => perms_match = self.can_write(ino_data.attrs().perm, ino_data.attrs().uid, ino_data.attrs().gid, req.uid(), req.gid()) && self.can_read(ino_data.attrs().perm, ino_data.attrs().uid, ino_data.attrs().gid, req.uid(), req.gid()),
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
        reply.error(EACCES);
    }

    fn write(&mut self, _req: &Request, inode: u64, fh: u64, offset: i64, data: &[u8], _write_flags: u32,flags: i32, _lock_owner: Option<u64>, reply: ReplyWrite) {
        println!("write(inode={}, fh={}, offset={}, len(data)={}, flags={})", inode, fh, offset, data.len(), flags);
        // Check if we can write:
        if (fh & FILE_HANDLE_WRITE_BIT) == 0 {
            reply.error(EACCES);
            return;
        }

        let mut ino_data = match self.get_inode(inode) {
            Some(a) => match a {
		Inode::FileInode(ref a) => Inode::FileInode(a.clone()),
		Inode::DirectoryInode(ref b) => Inode::DirectoryInode(b.clone()),
	    },
            None => {
                reply.error(EBADF);
                return;
            },
        };

        let cur_data = ino_data.data().to_string();
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
        ino_data.set_data(String::from_utf8(new_data).expect("Our bytes should be valid utf8"));

        let now = SystemTime::now();
        let mut attrs = ino_data.attrs().clone();
        attrs.mtime = now;
        attrs.atime = now;
        attrs.size = new_length as u64;
        attrs.blocks = (attrs.size + self.block_size as u64 - 1) / self.block_size as u64;
        ino_data.set_attrs(attrs);

        self.set_inode(inode, ino_data.clone());

        reply.written(data.len() as u32);
        return;
    }

    fn release(&mut self, _req: &Request<'_>, inode: u64, fh: u64, flags: i32, _lock_owner: Option<u64>, flush: bool, reply: ReplyEmpty) {
        println!("release(inode={}, fh={}, flags={}, flush={})", inode, fh, flags, flush);
        self.release_file_handle(inode);
        reply.ok();
    }

    fn unlink(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        println!("unlink(parent={}, name={:?})", parent, name);
        let mut ino_data = match self.get_inode(parent) {
            Some(a) => match a {
                Inode::FileInode(ref a) => Inode::FileInode(a.clone()),
                Inode::DirectoryInode(ref b) => Inode::DirectoryInode(b.clone()),
            },
            None => {
                reply.error(EBADF);
                return;
            }
        };
        if let perms = self.can_write(ino_data.attrs().perm, ino_data.attrs().uid, ino_data.attrs().gid, req.uid(), req.gid()) {
            // Update the metadata for the parent
            let now = SystemTime::now();
            let mut ino_attrs = ino_data.attrs().clone();
            ino_attrs.mtime = now;
            ino_attrs.atime = now;
            ino_data.set_attrs(ino_attrs);

            //Update the inode in the tree
            self.set_inode(parent, ino_data.clone());

            // Infer the path for the target
            //let parent_path = self.get_path_by_inode(parent);
            //let target_path = Path::new(&parent_path).join(name.to_str().expect("")).into_os_string().into_string().unwrap();
            for ino in ino_data.contents() {
                let cur = match self.get_inode(*ino) {
                    Some(a) => a.clone(),
                    None => todo!(),
                };

                if cur.name().clone() == name.to_string_lossy() {
                    self.remove_inode(*ino);
                    break;
                }
            }
            // Remove the target inode
            reply.ok();
            return;
        } else {
            reply.error(EACCES);
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
        let mut parent_inode = match self.get_inode(parent) {
            Some(a) => a.clone(),//match a {
	//	Inode::FileInode(ref a) => Inode::FileInode(a.clone()),
	//	Inode::DirectoryInode(ref b) => Inode::DirectoryInode(b.clone()),
	//    },
            None => {
                reply.error(EEXIST);
                return;
            },
        };

        let tmp_target_path = Path::new(parent_inode.path()).join(name);
        let target_path = tmp_target_path.to_string_lossy();
        
        // Check if path exists
        for ino in parent_inode.contents() {
            let cur = match self.get_inode(*ino) {
                Some(a) => a.clone(),
                //    Inode::FileInode(b) => Inode::FileInode(b.clone()),
                //    Inode::DirectoryInode(c) => Inode::DirectoryInode(c.clone()),
                //},
                None => todo!(),
            };

            if cur.path().clone() == *target_path {
                reply.error(EEXIST);
                return;
            }
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
        let mut parent_attrs = parent_inode.attrs().clone();
        let now = SystemTime::now();
        parent_attrs.mtime = now;
        parent_attrs.atime = now;
        parent_inode.set_attrs(parent_attrs);

        if self.can_write(parent_inode.attrs().perm, parent_inode.attrs().uid, parent_inode.attrs().gid, req.uid(), req.gid()) {
            let target_ino = match self.create_inode(target_path.to_string(), FileType::RegularFile, mode.try_into().unwrap(), 0, req.uid(), req.gid(), parent_inode.inode_num().clone(), "".to_string()) {
                Inode::FileInode(ref a) => Inode::FileInode(a.clone()),
                Inode::DirectoryInode(ref b) => Inode::DirectoryInode(b.clone()),
            };
            // Update the parent's contents
            let mut parent_contents = parent_inode.contents().clone();
            parent_contents.push(target_ino.inode_num());
            parent_inode.set_contents(parent_contents);
            self.set_inode(parent, parent_inode.clone());

            let fh = self.allocate_file_handle(target_ino.inode_num().clone(), read, write);

            reply.created(
                &Duration::new(0,0), 
                &target_ino.attrs(), 
                0, 
                fh.clone(), 
                0,
            );
            return;
        } else {
            reply.error(EACCES);
            return;
        }
        reply.error(EPERM);
    }

    fn rename(&mut self, req: &Request, parent: u64, name: &OsStr, new_parent: u64, new_name: &OsStr, flags: u32, reply: ReplyEmpty) {
        println!("rename(parent={}, name={}, new_parent={}, new_name={}, flags={})", parent, name.to_string_lossy(), new_parent, new_name.to_string_lossy(), flags);
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
        let mut parent_inode = match self.get_inode(parent) {
            Some(a) => match a {
                Inode::FileInode(ref a) => Inode::FileInode(a.clone()),
                Inode::DirectoryInode(ref b) => Inode::DirectoryInode(b.clone()),
            },
            None => {
                reply.error(EPERM);
                return;
            },
        };
        let mut new_parent_inode = match self.get_inode(new_parent) {
            Some(a) => match a {
                Inode::FileInode(ref a) => Inode::FileInode(a.clone()),
                Inode::DirectoryInode(ref b) => Inode::DirectoryInode(b.clone()),
            },
            None => {
                reply.error(EPERM);
                return;
            },
        };

        let tmp_source_path = Path::new(&parent_inode.path()).join(name);
        let source_path = tmp_source_path.to_string_lossy();
        let tmp_target_path = Path::new(&new_parent_inode.path()).join(new_name) ;
        let target_path = tmp_target_path.to_string_lossy();

        let mut source_ino = match self.get_inode_by_path(source_path.to_string()) {
            Some(a) => match a {
                Inode::FileInode(ref a) => Inode::FileInode(a.clone()),
                Inode::DirectoryInode(ref b) => Inode::DirectoryInode(b.clone()),
            },
            None => {
                reply.error(EPERM);
                return;
            },
        };

        // Make sure target path doesn't exist
        if self.get_inode_by_path(target_path.to_string()) != None {
            reply.error(EINVAL);
            return;
        }

        // Check that we can read the source, and write to the new parent
        if !self.can_read(source_ino.attrs().perm, source_ino.attrs().uid, source_ino.attrs().gid, req.uid(), req.gid()) ||
            !self.can_write(new_parent_inode.attrs().perm, new_parent_inode.attrs().uid, new_parent_inode.attrs().gid, req.uid(), req.gid()) {
            reply.error(EPERM);
            return;
        }

        //update the mtime/atime of the old and new parents
        let now = SystemTime::now();
        let mut source_attrs = source_ino.attrs().clone();
        let mut parent_attrs = parent_inode.attrs().clone();
        let mut new_parent_attrs = new_parent_inode.attrs().clone();

        source_attrs.mtime = now;
        source_attrs.atime = now;
        parent_attrs.mtime = now;
        parent_attrs.atime = now;
        new_parent_attrs.mtime = now;
        new_parent_attrs.atime = now;

        source_ino.set_attrs(source_attrs);
        parent_inode.set_attrs(parent_attrs);
        new_parent_inode.set_attrs(new_parent_attrs);

        //Remove from the parent_inode's contents
        // we use source_ino here b/c the inode number will not change
        let mut parent_contents = parent_inode.contents().clone();
        let mut new_parent_contents = new_parent_inode.contents().clone();
        parent_contents.remove(source_ino.inode_num().clone().try_into().unwrap());
        // Add inode to the new parent
        new_parent_contents.push(source_ino.inode_num().clone().try_into().unwrap());
        // Change the source_ino's parent
        source_ino.set_parent(new_parent_inode.inode_num().clone());
        parent_inode.set_contents(parent_contents);
        new_parent_inode.set_contents(new_parent_contents);

        //Update the parents
        self.set_inode(parent_inode.inode_num().clone(), parent_inode.clone());
        self.set_inode(new_parent_inode.inode_num().clone(), new_parent_inode.clone());
        self.set_inode(source_ino.inode_num().clone(), source_ino.clone());

        reply.ok();

    }

    fn link(&mut self, _req: &Request, inode: u64, new_parent: u64, new_name: &OsStr, reply: ReplyEntry) {
        println!("link(inode={}, new_parent={}, new_name={})", inode, new_parent, new_name.to_str().unwrap());
        reply.error(ENOSYS);
    }

    fn setattr(&mut self, req: &Request, inode: u64, mode: Option<u32>, uid: Option<u32>, gid: Option<u32>, size: Option<u64>, atime: Option<TimeOrNow>, mtime: Option<TimeOrNow>, _ctime: Option<SystemTime>, fh: Option<u64>, _crtime: Option<SystemTime>, _chgtime: Option<SystemTime>, _bkuptime: Option<SystemTime>, _flags: Option<u32>, reply: ReplyAttr) {
       println!("setattr(inode={:?}, mode={:?}, uid={:?}, gid={:?}, size={:?}, atime={:?}, mtime={:?}, fh={:?})", inode, mode, uid, gid, size, atime, mtime, fh);
       let mut ino_data = match self.get_inode(inode) {
           Some(a) => match a {
               Inode::FileInode(ref a) => Inode::FileInode(a.clone()),
               Inode::DirectoryInode(ref b) => Inode::DirectoryInode(b.clone()),
           },
           None => {
               reply.error(EPERM);
               return;
           },
       };

       // Check that we can write to the file
       if !self.can_write(ino_data.attrs().perm, ino_data.attrs().uid, ino_data.attrs().gid, req.uid(), req.gid()) {
           reply.error(EPERM);
           return;
       }

       let mut attrs = ino_data.attrs().clone();
       if let Some(m) = mode {
           attrs.perm = (m & !S_ISGID as u32) as u16;
       }
       if let Some(u) = uid {
           attrs.uid = u;
       }
       if let Some(g) = gid {
           attrs.gid = g;
       }
       if let Some(s) = size {
           attrs.size = s;
       }
       if let Some(a) = atime {
           if let TimeOrNow::Now = a {
               attrs.atime = SystemTime::now();
           } else if let TimeOrNow::SpecificTime(time) = a {
               attrs.atime = time;
           }
       }
       if let Some(t) = mtime {
           if let TimeOrNow::Now = t {
               attrs.mtime = SystemTime::now();
           } else if let TimeOrNow::SpecificTime(time) = t {
               attrs.mtime = time;
           }
       }

       ino_data.set_attrs(attrs.clone());

       self.set_inode(ino_data.inode_num(), ino_data.clone());
       reply.attr(&Duration::new(0, 0), &ino_data.attrs());
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
}

fn main() {
    let mut data = BTreeMap::new();
    data.insert("/foo".to_string(), "bar".to_string());
    data.insert("/answer".to_string(), "42".to_string());

    let fs = TreeFilesystem::new(&data);
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
