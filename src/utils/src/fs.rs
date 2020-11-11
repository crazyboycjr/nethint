use std::io::{Seek, Write};
use std::path::Path;
use std::sync::Mutex;
use lazy_static::lazy_static;

lazy_static! {
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
    let _file_mutex = FILE_MUTEX.lock().unwrap();

    let mut f = open_with_create_append(filename);
    f.seek(std::io::SeekFrom::End(0)).unwrap();
    writeln!(f, "{}", content).unwrap();
}
