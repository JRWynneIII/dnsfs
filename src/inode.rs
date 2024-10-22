use fuser::FileAttr;

#[derive(Debug,Clone,PartialEq)]
pub struct FileInode {
    pub inode_num: u64,
    pub attrs: FileAttr,
    pub path: String,
    pub data: Vec<u8>, //Should be base64 encoded
    pub num_links: u32,
    pub name: String,
    pub parent: u64,
}

#[derive(Debug,Clone,PartialEq)]
pub struct LinkInode {
    pub inode_num: u64,
    pub attrs: FileAttr,
    pub path: String,
    pub target: u64, 
    pub name: String,
    pub parent: u64,
    pub num_links: u32,
    pub target_path: String,
}

#[derive(Debug,Clone,PartialEq)]
pub struct DirectoryInode {
    pub inode_num: u64,
    pub attrs: FileAttr,
    pub path: String,
    pub contents: Vec<u64>, //List of inode numbers of contents
    pub num_links: u32,
    pub parent: u64,
    pub name: String,
}

#[derive(Debug,Clone,PartialEq)]
pub enum Inode {
    FileInode(FileInode),
    DirectoryInode(DirectoryInode),
    LinkInode(LinkInode),
}

pub trait InodeTrait {
    fn inode_num(&self) -> u64;
    fn attrs(&self) -> &FileAttr;
    fn path(&self) -> &String;
    fn data(&self) -> &Vec<u8>;
    #[allow(dead_code)]
    fn parent(&self) -> u64;
    fn name(&self) -> &String;
    fn contents(&self) -> &Vec<u64>;
    fn target(&self) -> Option<u64>;
    fn get_symlink_data(&self) -> Option<&String>;
    fn set_attrs(&mut self, _: FileAttr);
    #[allow(dead_code)]
    fn set_path(&mut self, _: String);
    #[allow(dead_code)]
    fn set_inode_num(&mut self, _: u64);
    fn set_parent(&mut self, _:u64);
    fn set_data(&mut self, _: Vec<u8>);
    fn set_contents(&mut self, _: Vec<u64>);
    fn set_link_target(&mut self, _: u64);
    fn set_symlink_data(&mut self, _: String);
}

impl InodeTrait for Inode {
    fn target(&self) -> Option<u64> {
        match self {
            Inode::FileInode(_) => return None,
            Inode::DirectoryInode(_) => return None,
            Inode::LinkInode(ref c) => return Some(c.target),
        }
    }

    fn inode_num(&self) -> u64 {
        match self {
            Inode::FileInode(ref a) => return a.inode_num.clone(),
            Inode::DirectoryInode(ref b) => return b.inode_num.clone(),
            Inode::LinkInode(ref c) => return c.inode_num.clone(),
        };
    }

    fn attrs(&self) -> &FileAttr {
        match self {
            Inode::FileInode(ref a) => return &a.attrs,
            Inode::DirectoryInode(ref b) => return &b.attrs,
            Inode::LinkInode(ref c) => return &c.attrs,
        };
    }

    fn path(&self) -> &String {
        match self {
            Inode::FileInode(ref a) => return &a.path,
            Inode::DirectoryInode(ref b) => return &b.path,
            Inode::LinkInode(ref c) => return &c.path,
        };
    }

    fn data(&self) -> &Vec<u8> {
        match self {
            Inode::FileInode(ref a) => return &a.data,
            Inode::DirectoryInode(_) => todo!(),
            Inode::LinkInode(_) => todo!(),
        };
    }

    fn get_symlink_data(&self) -> Option<&String> {
        match self {
            Inode::FileInode(_) => todo!(),
            Inode::DirectoryInode(_) => todo!(),
            Inode::LinkInode(ref c) => {
                if c.target_path.len() > 0 {
                    return Some(&(c.target_path));
                } else {
                    return None;
                }
            },
        };
    }

    fn parent(&self) -> u64 {
        match self {
            Inode::FileInode(ref a) => return a.parent.clone(),
            Inode::DirectoryInode(ref b) => return b.parent.clone(),
            Inode::LinkInode(ref c) => return c.parent.clone(),
        };
    }

    fn name(&self) -> &String {
        match self {
            Inode::FileInode(ref a) => return &a.name,
            Inode::DirectoryInode(ref b) => return &b.name,
            Inode::LinkInode(ref c) => return &c.name,
        };
    }

    fn contents(&self) -> &Vec<u64> {
        match self {
            Inode::FileInode(_) => todo!(),
            Inode::DirectoryInode(ref b) => return &b.contents,
            Inode::LinkInode(_) => todo!(),
        }
    }

    fn set_attrs(&mut self, attrs: FileAttr) {
        match self {
            Inode::FileInode(ref mut a) =>  a.attrs = attrs,
            Inode::DirectoryInode(ref mut b) =>  b.attrs = attrs,
            Inode::LinkInode(ref mut c) => c.attrs = attrs,
        };
    }

    fn set_path(&mut self, path: String) {
        match self {
            Inode::FileInode(ref mut a) =>  a.path = path,
            Inode::DirectoryInode(ref mut b) =>  b.path = path,
            Inode::LinkInode(ref mut c) => c.path = path,
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
            Inode::LinkInode(ref mut c) => {
                c.inode_num = ino.clone();
                c.attrs.ino = ino.clone();
            },
        };
    }

    fn set_parent(&mut self, parent: u64) {
        match self {
            Inode::FileInode(ref mut a) =>  a.parent = parent,
            Inode::DirectoryInode(ref mut b) =>  b.parent = parent,
            Inode::LinkInode(ref mut c) => c.parent = parent,
        };
    }

    fn set_link_target(&mut self, target: u64) {
        match self {
            Inode::FileInode(_) =>  todo!(),
            Inode::DirectoryInode(_) => todo!(),
            Inode::LinkInode(ref mut c) => c.target = target,
        };
    }

    fn set_data(&mut self, data: Vec<u8>) {
        match self {
            Inode::FileInode(ref mut a) =>  a.data = data.clone(),
            Inode::DirectoryInode(_) => todo!(),
            Inode::LinkInode(_) => todo!(),
        };
    }

    fn set_contents(&mut self, data: Vec<u64>) {
        match self {
            Inode::FileInode(_) =>  todo!(),
            Inode::DirectoryInode(ref mut a) => a.contents = data.clone(),
            Inode::LinkInode(_) => todo!(),
        };
    }

    fn set_symlink_data(&mut self, path: String) {
        match self {
            Inode::FileInode(_) => todo!(),
            Inode::DirectoryInode(_) => todo!(),
            Inode::LinkInode(ref mut c) => c.target_path = path.to_string(),
        };
    }
}

