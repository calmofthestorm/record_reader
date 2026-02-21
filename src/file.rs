use std::fs::*;
use std::io::*;
use std::path::Path;

use anyhow::{Context, Result};
use byteorder::{ByteOrder, NetworkEndian};

use crate::*;

pub struct IoRecordReader<R: Read> {
    fd: R,
    buf: Vec<u8>,
    format: Format,
    max_record_size: usize,
}

pub struct IoRecordWriter<W: Write> {
    fd: W,
    format: Format,
}

// IoRecordReader

impl IoRecordReader<std::fs::File> {
    pub fn open(path: &Path, format: Format, max_record_size: usize) -> Result<Self> {
        Ok(Self::from_read(File::open(path)?, format, max_record_size))
    }
}

impl<R: Read> IoRecordReader<R> {
    pub fn from_read(inner: R, format: Format, max_record_size: usize) -> Self {
        IoRecordReader {
            fd: inner,
            buf: vec![0; std::cmp::min(max_record_size, 8192)],
            max_record_size,
            format,
        }
    }

    pub fn into_inner(self) -> R {
        self.fd
    }
}

impl<R: Read> RecordReader for IoRecordReader<R> {
    fn maybe_read_record(&mut self) -> Result<Option<&[u8]>> {
        match self.format {
            Format::Chunk => {
                let mut buf = self.buf.as_mut_slice();

                while !buf.is_empty() {
                    match self.fd.read(buf) {
                        Ok(0) => break,
                        Ok(n) => {
                            buf = &mut buf[n..];
                        }
                        Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
                        Err(e) => return Err(e.into()),
                    }
                }

                let length = buf.len();
                let length = self.buf.len() - length;
                if length == 0 {
                    Ok(None)
                } else {
                    Ok(Some(&self.buf[..length]))
                }
            }
            Format::Record => {
                let mut length = [0; 8];

                let n = read_to_end_partial(&mut self.fd, &mut length[..8])?;
                if n == 8 {
                    let length = NetworkEndian::read_u64(&length) as usize;
                    if length > self.max_record_size {
                        anyhow::bail!("incomplete record (or buffer is too small)");
                    }
                    if length > self.buf.len() {
                        self.buf.resize(length, 0);
                    }
                    self.fd.read_exact(&mut self.buf[..length])?;
                    Ok(Some(&self.buf[..length]))
                } else if n == 0 {
                    Ok(None)
                } else {
                    Err(anyhow::anyhow!("incomplete record header"))
                }
            }
            Format::Record32 => {
                let mut length = [0; 4];

                let n = read_to_end_partial(&mut self.fd, &mut length[..4])?;
                if n == 4 {
                    let length = NetworkEndian::read_u32(&length) as usize;
                    if length > self.max_record_size {
                        anyhow::bail!("incomplete record (or buffer is too small)");
                    }
                    if length > self.buf.len() {
                        self.buf.resize(length, 0);
                    }
                    self.fd.read_exact(&mut self.buf[..length])?;
                    Ok(Some(&self.buf[..length]))
                } else if n == 0 {
                    Ok(None)
                } else {
                    anyhow::bail!("incomplete record header")
                }
            }
        }
    }
}

fn read_to_end_partial<R: Read>(fd: &mut R, mut buf: &mut [u8]) -> Result<usize> {
    let req = buf.len();

    while !buf.is_empty() {
        match fd.read(buf) {
            Ok(0) => break,
            Ok(n) => {
                let tmp = buf;
                buf = &mut tmp[n..];
            }
            Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }

    Ok(req - buf.len())
}

// IoRecordWriter

impl<W: Write> IoRecordWriter<W> {
    pub fn new(inner: W, format: Format) -> Self {
        IoRecordWriter { fd: inner, format }
    }

    #[must_use]
    pub fn into_inner(self) -> W {
        self.fd
    }
}

impl IoRecordWriter<BufWriter<File>> {
    pub fn create(path: &Path, format: Format) -> Result<Self> {
        let fd = BufWriter::new(File::create(path)?);
        Ok(IoRecordWriter { fd, format })
    }

    pub fn create_new(path: &Path, format: Format) -> Result<Self> {
        let fd = OpenOptions::new().create_new(true).write(true).open(path)?;
        let fd = BufWriter::new(fd);
        Ok(IoRecordWriter { fd, format })
    }
}

impl<Inner> RecordWriter for IoRecordWriter<Inner>
where
    Inner: Write,
{
    /// Write a record. Will not write records that exceed max_record_size.
    fn write_record(&mut self, data: &[u8]) -> Result<()> {
        match self.format {
            Format::Chunk => self.fd.write_all(data).map_err(Into::into),
            Format::Record => write_record(&mut self.fd, data),
            Format::Record32 => write_record32(&mut self.fd, data),
        }
    }

    fn flush(&mut self) -> Result<()> {
        self.fd.flush().context("flush IoRecordWriter")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    use crate::test_util::*;

    struct InterruptedThenReader {
        data: Vec<u8>,
        pos: usize,
        interrupted: bool,
    }

    impl InterruptedThenReader {
        fn new(data: &[u8]) -> Self {
            Self {
                data: data.to_vec(),
                pos: 0,
                interrupted: false,
            }
        }
    }

    impl Read for InterruptedThenReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if !self.interrupted {
                self.interrupted = true;
                return Err(std::io::Error::new(ErrorKind::Interrupted, "interrupted"));
            }
            if self.pos >= self.data.len() {
                return Ok(0);
            }
            let n = std::cmp::min(buf.len(), self.data.len() - self.pos);
            buf[..n].copy_from_slice(&self.data[self.pos..self.pos + n]);
            self.pos += n;
            Ok(n)
        }
    }

    #[test]
    fn read_to_end_partial_handles_interrupted() {
        let mut reader = InterruptedThenReader::new(&[1, 2, 3, 4]);
        let mut out = [0_u8; 4];
        let n = read_to_end_partial(&mut reader, &mut out).unwrap();
        assert_eq!(n, 4);
        assert_eq!(out, [1, 2, 3, 4]);
    }

    #[test]
    fn record_incomplete_header_returns_err() {
        let mut rr = IoRecordReader::from_read(Cursor::new(vec![0, 0, 0, 0]), Format::Record, 10);
        assert!(rr.maybe_read_record().is_err());
    }

    #[test]
    fn record32_incomplete_header_returns_err() {
        let mut rr = IoRecordReader::from_read(Cursor::new(vec![0, 0]), Format::Record32, 10);
        assert!(rr.maybe_read_record().is_err());
    }

    #[test]
    fn record_incomplete_payload_returns_err() {
        let mut buf = vec![0_u8; 8 + 2];
        NetworkEndian::write_u64(&mut buf[..8], 3);
        let mut rr = IoRecordReader::from_read(Cursor::new(buf), Format::Record, 10);
        assert!(rr.maybe_read_record().is_err());
    }

    #[test]
    fn record32_incomplete_payload_returns_err() {
        let mut buf = vec![0_u8; 4 + 1];
        NetworkEndian::write_u32(&mut buf[..4], 2);
        let mut rr = IoRecordReader::from_read(Cursor::new(buf), Format::Record32, 10);
        assert!(rr.maybe_read_record().is_err());
    }

    #[test]
    fn create_new_refuses_existing_path() {
        let td = tempfile::TempDir::with_prefix("rust-test").unwrap();
        let f1 = td.path().join("f1");

        let _w = IoRecordWriter::create_new(&f1, Format::Chunk).unwrap();
        assert!(IoRecordWriter::create_new(&f1, Format::Chunk).is_err());
    }

    #[test]
    fn reader_into_inner_returns_reader() {
        let rr = IoRecordReader::from_read(Cursor::new(b"xyz".to_vec()), Format::Chunk, 8);
        let mut inner = rr.into_inner();
        let mut out = Vec::new();
        inner.read_to_end(&mut out).unwrap();
        assert_eq!(out, b"xyz");
    }

    #[test]
    fn writer_into_inner_returns_written_bytes() {
        let mut w = IoRecordWriter::new(Vec::new(), Format::Chunk);
        w.write_record(b"abc").unwrap();
        w.flush().unwrap();
        let out = w.into_inner();
        assert_eq!(out, b"abc");
    }

    fn file_file(format: Format) {
        // Yes, we can only do one at a time.
        let td = tempfile::TempDir::with_prefix("rust-test").unwrap();
        let f1 = td.path().join("f1");

        test_general(
            format,
            |format| IoRecordWriter::create(&f1, format).unwrap(),
            |_, format, max_read_size| IoRecordReader::open(&f1, format, max_read_size).unwrap(),
        );
        match format {
            Format::Record | Format::Record32 => {
                test_records_toobig_format(
                    format,
                    |format| IoRecordWriter::create(&f1, format).unwrap(),
                    |_, format, max_read_size| {
                        IoRecordReader::open(&f1, format, max_read_size).unwrap()
                    },
                );
                test_records_max_size_boundary(
                    format,
                    |format| IoRecordWriter::create(&f1, format).unwrap(),
                    |_, format, max_read_size| {
                        IoRecordReader::open(&f1, format, max_read_size).unwrap()
                    },
                );
            }
            Format::Chunk => {}
        }
    }

    #[test]
    fn file_file_records() {
        file_file(Format::Record)
    }

    #[test]
    fn file_file_chunks() {
        file_file(Format::Chunk)
    }

    #[test]
    fn file_file_records32() {
        file_file(Format::Record32)
    }

    fn file_memory(format: Format) {
        // Yes, we can only do one at a time.
        let td = tempfile::TempDir::with_prefix("rust-test").unwrap();
        let f1 = td.path().join("f1");

        test_general(
            format,
            |format| IoRecordWriter::create(&f1, format).unwrap(),
            |fw, format, max_read_size| {
                std::mem::drop(fw);
                let max_read_size = match format {
                    Format::Chunk => max_read_size,
                    Format::Record => max_read_size,
                    Format::Record32 => max_read_size,
                };
                BufferRecordReader::new(std::fs::read(&f1).unwrap().into(), format, max_read_size)
            },
        );
        match format {
            Format::Record | Format::Record32 => {
                test_records_toobig_format(
                    format,
                    |format| IoRecordWriter::create(&f1, format).unwrap(),
                    |fw, format, max_read_size| {
                        std::mem::drop(fw);
                        BufferRecordReader::new(
                            std::fs::read(&f1).unwrap().into(),
                            format,
                            max_read_size,
                        )
                    },
                );
                test_records_max_size_boundary(
                    format,
                    |format| IoRecordWriter::create(&f1, format).unwrap(),
                    |fw, format, max_read_size| {
                        std::mem::drop(fw);
                        BufferRecordReader::new(
                            std::fs::read(&f1).unwrap().into(),
                            format,
                            max_read_size,
                        )
                    },
                );
            }
            Format::Chunk => {}
        }
    }

    #[test]
    fn file_memory_records() {
        file_memory(Format::Record)
    }

    #[test]
    fn file_memory_chunks() {
        file_memory(Format::Chunk)
    }

    #[test]
    fn file_memory_records32() {
        file_memory(Format::Record32)
    }

    fn memory_file(format: Format) {
        // Yes, we can only do one at a time.
        let td = tempfile::TempDir::with_prefix("rust-test").unwrap();
        let f1 = td.path().join("f1");

        let writer = |format| BufferRecordWriter::new(format);
        let reader = |vrw: BufferRecordWriter, format, max_read_size| {
            let v: Vec<_> = vrw.into();
            std::fs::write(&f1, &v).unwrap();
            IoRecordReader::open(&f1, format, max_read_size).unwrap()
        };

        test_general(format, writer, reader);
        match format {
            Format::Record | Format::Record32 => {
                test_records_toobig_format(format, writer, reader);
                test_records_max_size_boundary(format, writer, reader);
            }
            Format::Chunk => {}
        }
    }

    #[test]
    fn memory_file_records() {
        memory_file(Format::Record)
    }

    #[test]
    fn memory_file_chunks() {
        memory_file(Format::Chunk)
    }

    #[test]
    fn memory_file_records32() {
        memory_file(Format::Record32)
    }
}
