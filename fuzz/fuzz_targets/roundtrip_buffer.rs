#![no_main]

use libfuzzer_sys::fuzz_target;
use record_reader::{BufferRecordReader, BufferRecordWriter, Format, RecordReader, RecordWriter};

fn split_records(bytes: &[u8], count_hint: usize) -> Vec<Vec<u8>> {
    if bytes.is_empty() || count_hint == 0 {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut i = 0usize;
    let mut remaining = count_hint;
    while i < bytes.len() && remaining > 0 {
        let len_seed = usize::from(bytes[i]);
        i += 1;
        let remaining_bytes = bytes.len() - i;
        let len = if remaining_bytes == 0 {
            0
        } else {
            len_seed % (remaining_bytes + 1)
        };
        out.push(bytes[i..i + len].to_vec());
        i += len;
        remaining -= 1;
    }
    out
}

fuzz_target!(|data: &[u8]| {
    if data.len() < 4 {
        return;
    }

    let format = match data[0] % 3 {
        0 => Format::Chunk,
        1 => Format::Record,
        _ => Format::Record32,
    };
    let count_hint = (usize::from(data[1]) % 16) + 1;
    let max_read_size = (usize::from(data[2]) % 64) + 1;
    let records = split_records(&data[3..], count_hint);

    let mut w = BufferRecordWriter::new(format);
    for r in &records {
        let _ = w.write_record(r);
    }

    let mut rr = BufferRecordReader::new(w.into_cow(), format, max_read_size);

    match format {
        Format::Record | Format::Record32 => {
            let mut got = Vec::new();
            while let Ok(Some(r)) = rr.maybe_read_record() {
                got.push(r.to_vec());
            }

            // Record readers fail on the first oversized record and stop.
            let expected: Vec<Vec<u8>> = records
                .iter()
                .take_while(|r| r.len() <= max_read_size)
                .cloned()
                .collect();
            assert_eq!(got, expected);
        }
        Format::Chunk => {
            let mut got = Vec::new();
            while let Ok(Some(r)) = rr.maybe_read_record() {
                got.extend_from_slice(r);
            }
            let expected: Vec<u8> = records.into_iter().flatten().collect();
            assert_eq!(got, expected);
        }
    }
});
