This `Rust` script can parse a 120G `WikiData` gzip dump in 50 minutes (40MB/s) into 8 shards of gzipped Messagepack streams, 
that can be read in `Python` in 20 seconds flat. The processing requires barely any memory (~100MB).

The measurements were done on a AMD Ryzen 5950x cpu on a Samsung NVME ssd.

The script currently also supports throwing out:

- unrequited languages 
- Disambiguation, list and "name" pages.
- Insignificant Chemical compounds and Astronomical objects (about 66% of whole wikidata) 
- garbage properties (about 3000 external IDs) 


# Running

     cargo run --release


To specify the location of the Wikidata dump edit the `main.rs` file.  
To change the filtering behaviour, edit the `wd_read.rs` or the `constants.rs` files.

As a learning project, this had not much organizing done.
