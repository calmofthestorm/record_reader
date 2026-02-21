#![no_main]

use libfuzzer_sys::fuzz_target;
use record_reader::{util::maybe_read_record_from_buffer, Format};

fuzz_target!(|data: &[u8]| {
    if data.len() < 8 {
        return;
    }

    let format = match data[0] % 3 {
        0 => Format::Chunk,
        1 => Format::Record,
        _ => Format::Record32,
    };
    let mut offset = usize::from(data[1]) | (usize::from(data[2]) << 8);
    let mut i = 3usize;
    let bytes = &data[7..];

    if !bytes.is_empty() {
        offset %= bytes.len();
    } else {
        offset = 0;
    }

    while i + 1 < data.len() && i < 64 {
        let requested_size = usize::from(data[i]) | (usize::from(data[i + 1]) << 8);
        i += 2;
        let max_record_size = usize::from(data[i % data.len()]) % 128;

        let prev = offset;
        let result = maybe_read_record_from_buffer(
            bytes,
            &mut offset,
            requested_size,
            max_record_size,
            format,
        );

        assert!(offset <= bytes.len());
        if matches!(result, Ok(Some(_))) {
            assert!(offset > prev);
        }
    }
});
