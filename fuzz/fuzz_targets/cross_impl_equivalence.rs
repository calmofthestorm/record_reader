#![no_main]

use std::io::Cursor;

use libfuzzer_sys::fuzz_target;
use record_reader::{
    BufferRecordReader, BufferRecordWriter, Format, IoRecordReader, RecordReader, RecordWriter,
};

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

#[derive(Debug, PartialEq, Eq)]
enum Terminal {
    Eof,
    Err,
}

fn drain_records<R: RecordReader>(rr: &mut R) -> (Vec<Vec<u8>>, Terminal) {
    let mut out = Vec::new();
    loop {
        match rr.maybe_read_record() {
            Ok(Some(r)) => out.push(r.to_vec()),
            Ok(None) => return (out, Terminal::Eof),
            Err(_) => return (out, Terminal::Err),
        }
    }
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
    let bytes = w.into_cow().into_owned();

    let mut br = BufferRecordReader::new(bytes.clone().into(), format, max_read_size);
    let mut ir = IoRecordReader::from_read(Cursor::new(bytes), format, max_read_size);

    let (bout, bterm) = drain_records(&mut br);
    let (iout, iterm) = drain_records(&mut ir);

    assert_eq!(bout, iout);
    assert_eq!(bterm, iterm);
});
