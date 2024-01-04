
use std::convert::Into;
use std::fs::File;
use std::io::{BufReader, Error, SeekFrom};
use std::time::{Duration, Instant};
use std::io::prelude::*;
use rayon::prelude::*;


use std::iter::{Iterator};
use flate2::read::MultiGzDecoder;

use std::sync::{Arc, Mutex};
use indicatif::{ProgressBar, ProgressStyle};
use rmp_serde::to_vec_named;
use serde_json::Value;
use crate::{sharded, wd};
use crate::constants::{DISABLED_ALWAYS, DISABLED_PROPS, DISABLED_UNLINKED};


fn jtm(json_bytes: &[u8]) -> Option<Vec<u8>> {
    let mut item: wd::WikidataItem = serde_json::from_slice(&json_bytes).unwrap();

    let has_sitelinks: bool = item.sitelinks.len() > 0;
    if !has_sitelinks {
        let suspicious_props = item.claims.iter().filter(|c| {
            if c.contains_key("property") {
                let y: String = match &c["property"] {
                    Value::String(s) => { s.into() }
                    _ => { "".to_string() }
                };
                DISABLED_UNLINKED.contains(&&*y)
            } else {
                false
            }
        }).count();
        if suspicious_props > 0 {
            return None;
        }
    }

    if item.claims.iter().filter(|c| {
        if c.contains_key("property") & c.contains_key("id") {
            let x = c.get("property").unwrap() == "P31";
            if x {
                let y: String = match &c["id"] {
                    Value::String(s) => { s.into() }
                    _ => { "".to_string() }
                };
                // println!("InstanceOf: {:?} {:?} - {:?} {:?}", x, DISABLED.contains(&&*y), y, DISABLED);
                let always = DISABLED_ALWAYS.contains(&&*y);
                if has_sitelinks {
                    return always;
                } else {
                    return always || DISABLED_UNLINKED.contains(&&*y);
                }
            }
        }
        false
    }).count() > 0 {
        return None;
    }

    item.claims.retain(|e| {
        if e.contains_key("property") {
            let y: String = match &e["property"] {
                Value::String(s) => { s.into() }
                _ => { "".to_string() }
            };
            return !DISABLED_PROPS.contains(&&*y);
        }
        true
    });
    item.labels.retain(|e| e.language.starts_with("en"));
    item.descriptions.retain(|e| e.language.starts_with("en"));
    item.aliases.retain(|e| e.language.starts_with("en"));
    // item.sitelinks.retain(|e| e.site.starts_with("en"));
    if item.labels.len() == 0 {
        return None;
    }
    Some(to_vec_named(&item).unwrap())
}


pub(crate) fn read_json_from_gzip(file_path: &str, shard_name: &str, shard_count: usize) -> Result<(), Error> {
    let start = Instant::now();

    let file = Arc::new(Mutex::new(File::open(file_path)?));
    let file_size = file.lock().unwrap().metadata()?.len() / 1_000_000;

    let gz_decoder = MultiGzDecoder::new(file.lock().unwrap().try_clone()?);
    let reader = BufReader::new(gz_decoder);

    let encoder = sharded::ShardWriter::new(shard_count, shard_name)?;

    let pb = ProgressBar::new(file_size);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})").unwrap(),
    );
    pb.enable_steady_tick(Duration::new(1, 0));

    reader
        .lines()
        .filter_map(Result::ok)
        .map(|line: String| {
            pb.set_position(file.lock().unwrap().seek(SeekFrom::Current(0)).unwrap() / 1_000_000); // Set position as progress
            line
        })
        .par_bridge()
        .map(|line| line.trim_end_matches(|c| c == '[' || c == ']' || c == ',').into())
        .filter(|line: &String| line.len() > 5)
        .filter_map(|line: String| jtm(line.as_bytes()))
        .for_each(|wikidata_item| encoder.write_all(&*wikidata_item).unwrap())
        ;
    println!("Time elapsed in MP() is: {:?}", start.elapsed());
    Ok(())
}
