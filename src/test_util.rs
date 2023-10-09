use crate::*;

pub fn test_general<WF, RF, W, R>(format: Format, wf: WF, rf: RF)
where
    WF: Fn(Format) -> W,
    RF: Fn(W, Format, usize) -> R,
    W: RecordWriter,
    R: RecordReader,
{
    match format {
        Format::Chunk => test_chunks(wf, rf),
        Format::Record => test_records(wf, rf, format),
        Format::Record32 => test_records(wf, rf, format),
    }
}

pub fn test_records_toobig<WF, RF, W, R>(wf: WF, rf: RF)
where
    WF: Fn(Format) -> W,
    RF: Fn(W, Format, usize) -> R,
    W: RecordWriter,
    R: RecordReader,
{
    let words = record_vector(&["123456789"]);
    let mut w = wf(Format::Record);
    write_records(&mut w, &words);
    assert!(rf(w, Format::Record, 5).maybe_read_record().is_err());
}

fn test_records<WF, RF, W, R>(wf: WF, rf: RF, format: Format)
where
    WF: Fn(Format) -> W,
    RF: Fn(W, Format, usize) -> R,
    W: RecordWriter,
    R: RecordReader,
{
    for words in &[
        Vec::new(),
        record_vector(&[""]),
        record_vector(&["so, hi"]),
        record_vector(&["hello", "world", "", " ", "a", "a", "abba"]),
    ] {
        let mut w = wf(format);
        let max_read_size = words.iter().map(String::len).max().unwrap_or(0);
        write_records(&mut w, &words);
        assert_eq!(read_records(&mut rf(w, format, max_read_size)), *words);
    }
}

fn test_chunks<WF, RF, W, R>(wf: WF, rf: RF)
where
    WF: Fn(Format) -> W,
    RF: Fn(W, Format, usize) -> R,
    W: RecordWriter,
    R: RecordReader,
{
    for data in &[
        "I am having a good day",
        "",
        " ",
        "\n",
        "a",
        "This is a long string of text to test chunking thoroughly enough and I'm not feeling creative.",
    ] {
        let sizes = &[1, 2, 3, 5, 7, 10, 21, 1000];
        for write_size in sizes {
            for read_size in sizes {
                let mut w = wf(Format::Chunk);
                write_chunks(&mut w, data, *write_size);
                assert_eq!(&read_chunks(&mut rf(w, Format::Chunk, *read_size), *read_size), data);
            }
        }
    }
}

fn write_chunks<RW: RecordWriter>(rw: &mut RW, mut data: &str, max_write_size: usize) {
    while !data.is_empty() {
        let write_size = std::cmp::min(max_write_size, data.len());
        rw.write_record(data[..write_size].as_bytes()).unwrap();
        data = &data[write_size..];
    }
}

fn read_chunks<RR: RecordReader>(rr: &mut RR, max_read_size: usize) -> String {
    let mut s = String::default();
    while let Some(record) = rr.maybe_read_record().unwrap() {
        assert!(record.len() <= max_read_size);
        s.push_str(std::str::from_utf8(record).unwrap());
    }
    s
}

fn record_vector(records: &[&str]) -> Vec<String> {
    records.iter().map(|s| s.to_string()).collect()
}

fn read_records<RR: RecordReader>(rr: &mut RR) -> Vec<String> {
    let mut records = Vec::new();
    while let Some(record) = rr.maybe_read_record().unwrap() {
        records.push(std::str::from_utf8(record).unwrap().to_string());
    }
    records
}

fn write_records<RW: RecordWriter>(rw: &mut RW, records: &[String]) {
    for record in records.iter() {
        rw.write_record(record.as_bytes()).unwrap();
    }
}
