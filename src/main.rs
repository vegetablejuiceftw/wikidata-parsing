use std::io;

mod wd;
mod mp;
mod wd_read;
mod mp_read;
mod sharded;
mod constants;

fn main() -> io::Result<()> {
    // wd_read::read_json_from_gzip("100000.json.gz", "data/shard-mini-", 8)?;
    wd_read::read_json_from_gzip("latest-all.json.gz", "data/shard-16-", 16)?;
    Ok(())
}
