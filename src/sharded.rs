use std::fs::{create_dir_all, File};
use std::io::{self, Write};
use flate2::write::GzEncoder;
use flate2::Compression;
use std::sync::{Arc, Mutex};

pub(crate) struct ShardWriter {
    shard_writers: Vec<Arc<Mutex<GzEncoder<File>>>>,
}


impl ShardWriter {
    pub(crate) fn new(shard_count: usize, shard_prefix: &str) -> io::Result<Self> {
        if shard_prefix.contains('/') {
            create_dir_all(shard_prefix.split('/').collect::<Vec<_>>()[0])?;
        }
        let mut shard_writers = Vec::with_capacity(shard_count);
        for i in 0..shard_count {
            let shard_filename = format!("{}{}.gz", shard_prefix, i);
            let file = File::create(&shard_filename)?;
            let encoder = GzEncoder::new(file, Compression::fast());
            shard_writers.push(Arc::new(Mutex::new(encoder)));
        }
        Ok(ShardWriter { shard_writers })
    }

    pub(crate) fn write_all(&self, data: &[u8]) -> io::Result<()> {
        let shard_index = rand::random::<usize>() % self.shard_writers.len(); // Choose a random shard index
        self.shard_writers[shard_index].lock().unwrap().write_all(data)?;
        Ok(())
    }
}
