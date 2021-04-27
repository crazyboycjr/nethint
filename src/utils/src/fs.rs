pub fn open_with_create_append<P: AsRef<std::path::Path>>(path: P) -> std::fs::File {
    std::fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(&path)
        .unwrap_or_else(|e| panic!("fail to open or create {:?}: {}", path.as_ref(), e))
}