#![no_main]

use libfuzzer_sys::fuzz_target;
use record_reader::{
    BufferRecordReader, BufferRecordWriter, Format, HashingRecordReader, HashingRecordWriter,
    RecordReader, RecordWriter,
};
use sha2::{Digest, Sha256};

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
    if data.len() < 3 {
        return;
    }

    let count_hint = (usize::from(data[0]) % 16) + 1;
    let max_read_size = (usize::from(data[1]) % 128) + 1;
    let records = split_records(&data[2..], count_hint);

    let mut expected_hasher = Sha256::new();
    for r in &records {
        expected_hasher.update(r);
    }
    let expected = expected_hasher.finalize();

    let inner = BufferRecordWriter::new(Format::Chunk);
    let mut hw = HashingRecordWriter::new(inner, Sha256::new()).unwrap();
    for r in &records {
        let _ = hw.write_record(r);
    }
    let _ = hw.flush();
    let writer_hash = hw.finalize();

    let mut plain = BufferRecordWriter::new(Format::Chunk);
    for r in &records {
        let _ = plain.write_record(r);
    }

    let mut hr = HashingRecordReader::new(
        BufferRecordReader::new(plain.into_cow(), Format::Chunk, max_read_size),
        Sha256::new(),
    )
    .unwrap();
    let mut got = Vec::new();
    while let Ok(Some(r)) = hr.maybe_read_record() {
        got.extend_from_slice(r);
    }
    let reader_hash = hr.finalize();

    let expected_stream: Vec<u8> = records.into_iter().flatten().collect();
    assert_eq!(got, expected_stream);
    assert_eq!(writer_hash.as_slice(), expected.as_slice());
    assert_eq!(reader_hash.as_slice(), expected.as_slice());
});
