#![no_main]

use std::io::Cursor;

use libfuzzer_sys::fuzz_target;
use record_reader::{Format, IoRecordReader, RecordReader};

fuzz_target!(|data: &[u8]| {
    if data.len() < 6 {
        return;
    }

    let format = match data[0] % 3 {
        0 => Format::Chunk,
        1 => Format::Record,
        _ => Format::Record32,
    };
    let max_record_size = usize::from(data[1]) | (usize::from(data[2]) << 8);
    let max_iters = (usize::from(data[3]) % 64) + 1;
    let payload = &data[4..];

    let cursor = Cursor::new(payload.to_vec());
    let mut rr = IoRecordReader::from_read(cursor, format, max_record_size);

    for _ in 0..max_iters {
        match rr.maybe_read_record() {
            Ok(Some(record)) => {
                assert!(record.len() <= max_record_size);
                if matches!(format, Format::Chunk) {
                    assert!(!record.is_empty());
                }
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }
});
