use std::io::{self, Read, Seek, SeekFrom};
use std::fs::File;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use flate2::read::GzDecoder;
use indicatif::{ProgressBar, ProgressStyle};
use crate::mp::WikidataMessage;


#[allow(dead_code)]
pub(crate) fn read_wiki_from_mp(file_path: &str) -> io::Result<()> {
    const CHUNK_SIZE: usize = 1024 * 1024 * 1;

    let start = Instant::now();

    let file = Arc::new(Mutex::new(File::open(file_path)?));
    let file_size = file.lock().unwrap().metadata()?.len() / 1_000_000;
    let mut reader = GzDecoder::new(file.lock().unwrap().try_clone()?);

    let mut buffer = Vec::with_capacity(CHUNK_SIZE);
    let mut remaining_bytes: Vec<u8> = Vec::new();

    let mut count = 0;
    let mut bytes = 0;

    let pb = ProgressBar::new(file_size);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})").unwrap(),
    );
    pb.enable_steady_tick(Duration::new(1, 0));

    loop {
        buffer.clear();
        let bytes_read = reader.by_ref().take(CHUNK_SIZE as u64).read_to_end(&mut buffer)?;
        bytes += bytes_read;
        // println!("loop {:?} {:?}", count, bytes_read);

        if bytes_read == 0 && remaining_bytes.is_empty() {
            break; // End of file
        }

        // // Combine remaining bytes from the previous chunk, if any
        let mut combined_buffer = remaining_bytes.clone();
        combined_buffer.extend_from_slice(&buffer);

        let mut cursor = io::Cursor::new(combined_buffer.clone());
        let mut pos = 0;

        while (cursor.position() as usize) < combined_buffer.len() {
            let _x: WikidataMessage = match rmp_serde::from_read(&mut cursor) {
                Ok(value) => {
                    count += 1;
                    pos = cursor.position() as usize;
                    value
                }
                Err(_err) => {
                    // println!("Err: {:?}", err);
                    break;
                }
            };
        }
        remaining_bytes.clear();
        remaining_bytes.extend_from_slice(&combined_buffer[pos..]);
        pb.set_position(file.lock().unwrap().seek(SeekFrom::Current(0)).unwrap() / 1_000_000); // Set position as progress
    }

    println!("Count {:?}", count);
    println!("Bytes {:?}", bytes);
    println!("MBytes {:?}", bytes / 1000 / 1000);
    println!("Time elapsed is: {:?}", start.elapsed());
    let speed: f32 = (file_size as f32) / (start.elapsed().as_millis() as f32) * 1000.0;
    println!("Speed is: {:?}", speed);
    Ok(())
}
