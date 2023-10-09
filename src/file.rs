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
    fn maybe_read_record<'a>(&'a mut self) -> Result<Option<&'a [u8]>> {
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
                    None.context("incomplete record header")
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
    fn write_record<'a>(&'a mut self, data: &[u8]) -> Result<()> {
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

    use crate::test_util::*;

    fn file_file(format: Format) {
        // Yes, we can only do one at a time.
        let td = tempdir::TempDir::new("rust-test").unwrap();
        let f1 = td.path().join("f1");

        test_general(
            format,
            |format| IoRecordWriter::create(&f1, format).unwrap(),
            |_, format, max_read_size| IoRecordReader::open(&f1, format, max_read_size).unwrap(),
        );
    }

    #[test]
    fn file_file_records() {
        file_file(Format::Record)
    }

    #[test]
    fn file_file_chunks() {
        file_file(Format::Chunk)
    }

    fn file_memory(format: Format) {
        // Yes, we can only do one at a time.
        let td = tempdir::TempDir::new("rust-test").unwrap();
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
    }

    #[test]
    fn file_memory_records() {
        file_memory(Format::Record)
    }

    #[test]
    fn file_memory_chunks() {
        file_memory(Format::Chunk)
    }

    fn memory_file(format: Format) {
        // Yes, we can only do one at a time.
        let td = tempdir::TempDir::new("rust-test").unwrap();
        let f1 = td.path().join("f1");

        let writer = |format| BufferRecordWriter::new(format);
        let reader = |vrw: BufferRecordWriter, format, max_read_size| {
            let v: Vec<_> = vrw.into();
            std::fs::write(&f1, &v).unwrap();
            IoRecordReader::open(&f1, format, max_read_size).unwrap()
        };

        test_general(format, writer, reader);
        test_records_toobig(writer, reader);
    }

    #[test]
    fn memory_file_records() {
        memory_file(Format::Record)
    }

    #[test]
    fn memory_file_chunks() {
        memory_file(Format::Chunk)
    }
}
