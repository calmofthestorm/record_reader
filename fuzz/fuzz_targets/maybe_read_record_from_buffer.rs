#![no_main]

use libfuzzer_sys::fuzz_target;
use record_reader::{util::maybe_read_record_from_buffer, Format};

fuzz_target!(|data: &[u8]| {
    if data.len() < 9 {
        return;
    }

    let format = match data[0] % 3 {
        0 => Format::Chunk,
        1 => Format::Record,
        _ => Format::Record32,
    };

    let requested_size = usize::from(data[1]) | (usize::from(data[2]) << 8);
    let max_record_size = usize::from(data[3]) | (usize::from(data[4]) << 8);
    let mut offset = usize::from(data[5]) | (usize::from(data[6]) << 8);
    let iterations = (usize::from(data[7]) % 16) + 1;
    let bytes = &data[8..];

    // Keep offset in range for the first read; parser behavior after reads is asserted below.
    if !bytes.is_empty() {
        offset %= bytes.len();
    } else {
        offset = 0;
    }

    for _ in 0..iterations {
        let prev_offset = offset;
        let result = maybe_read_record_from_buffer(
            bytes,
            &mut offset,
            requested_size,
            max_record_size,
            format,
        );

        match result {
            Ok(Some(record)) => {
                assert!(!record.is_empty() || !matches!(format, Format::Chunk));
                assert!(offset >= prev_offset);
                assert!(offset <= bytes.len());
            }
            Ok(None) => {
                assert!(offset <= bytes.len());
                break;
            }
            Err(_) => {
                assert!(offset <= bytes.len());
                break;
            }
        }
    }
});
