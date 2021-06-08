use std::io::{Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use lazy_static::lazy_static;
use std::collections::HashMap;

lazy_static! {
    // static ref FILE_MUTEX: Mutex<HashMap<PathBuf, Mutex<()>>> = Mutex::new(Default::default());
    static ref FILE_MUTEX: Mutex<()> = Mutex::new(());
}

pub fn open_with_create_append<P: AsRef<std::path::Path>>(path: P) -> std::fs::File {
    std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(&path)
        .unwrap_or_else(|e| panic!("fail to open or create {:?}: {}", path.as_ref(), e))
}

pub fn append_to_file<P: AsRef<Path>>(filename: P, content: &str) {
    // let canonical_name = filename.as_ref().canonicalize().unwrap();
    // file_mutex.entry(canonical_name).or_insert(Mutex::new(()))
    {
        let _file_mutex = FILE_MUTEX.lock().unwrap();

        let mut f = open_with_create_append(filename);
        f.seek(std::io::SeekFrom::End(0)).unwrap();
        writeln!(f, "{}", content).unwrap();
    }
}
