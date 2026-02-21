/// Structs for working with single records and buffers.
use std::borrow::Cow;

use anyhow::Result;

use crate::*;

/// Concatenates all records into a single buffer.
#[derive(Clone)]
pub struct BufferRecordWriter {
    records: Vec<u8>,
    format: Format,
}

/// Splits a buffer into records.
#[derive(Clone)]
pub struct BufferRecordReader<'a> {
    records: Cow<'a, [u8]>,
    offset: usize,
    max_record_size: usize,
    format: Format,
}

// BufferRecordWriter

impl RecordWriter for BufferRecordWriter {
    fn write_record(&mut self, data: &[u8]) -> Result<()> {
        match self.format {
            Format::Record32 => {
                let offset = self.records.len();
                self.records.resize(offset + 4, 0);
                let len: u32 = data
                    .len()
                    .try_into()
                    .context("record length doesn't fit into size bytes.")?;
                NetworkEndian::write_u32(&mut self.records[offset..], len);
            }
            Format::Record => {
                let offset = self.records.len();
                self.records.resize(offset + 8, 0);
                let len: u64 = data
                    .len()
                    .try_into()
                    .context("record length doesn't fit into size bytes.")?;
                NetworkEndian::write_u64(&mut self.records[offset..], len);
            }
            Format::Chunk => {}
        }
        self.records.extend_from_slice(data);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

impl BufferRecordWriter {
    pub fn new(format: Format) -> BufferRecordWriter {
        Self::with_capacity(format, 0)
    }

    pub fn with_capacity(format: Format, capacity: usize) -> BufferRecordWriter {
        Self::from_vec(format, Vec::with_capacity(capacity))
    }

    pub fn from_vec(format: Format, inner: Vec<u8>) -> BufferRecordWriter {
        BufferRecordWriter {
            records: inner,
            format,
        }
    }

    pub fn into_cow(self) -> Cow<'static, [u8]> {
        self.records.into()
    }
}

impl From<BufferRecordWriter> for Vec<u8> {
    fn from(val: BufferRecordWriter) -> Self {
        val.records
    }
}

// BufferRecordReader

impl RecordReader for BufferRecordReader<'_> {
    fn maybe_read_record(&mut self) -> Result<Option<&[u8]>> {
        maybe_read_record_from_buffer(
            &self.records,
            &mut self.offset,
            self.records.len(),
            self.max_record_size,
            self.format,
        )
    }
}

impl BufferRecordReader<'_> {
    pub fn new<'a>(
        records: Cow<'a, [u8]>,
        format: Format,
        max_record_size: usize,
    ) -> BufferRecordReader<'a> {
        BufferRecordReader {
            records,
            offset: 0,
            format,
            max_record_size,
        }
    }

    pub fn from_vec(
        v: Vec<u8>,
        format: Format,
        max_record_size: usize,
    ) -> BufferRecordReader<'static> {
        Self::new(v.into(), format, max_record_size)
    }
}

impl<'a> BufferRecordReader<'a> {
    pub fn into_owned(self) -> BufferRecordReader<'static> {
        let v = match self.records {
            _ if self.offset >= self.records.len() => Cow::default(),
            Cow::Borrowed(b) => Cow::Owned(b.to_vec()),
            Cow::Owned(v) => Cow::Owned(v),
        };

        BufferRecordReader {
            records: v,
            offset: self.offset,
            format: self.format,
            max_record_size: self.max_record_size,
        }
    }
}

// Note that most other implementations test themselves against this struct,
// hence the comparatively light coverage here.
#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_util::*;

    fn buffer_buffer(format: Format) {
        test_general(
            format,
            BufferRecordWriter::new,
            |writer, format, max_read_size| {
                BufferRecordReader::new(writer.into_cow(), format, max_read_size)
            },
        );
        match format {
            Format::Record | Format::Record32 => {
                test_records_toobig_format(
                    format,
                    BufferRecordWriter::new,
                    |writer, format, max_read_size| {
                        BufferRecordReader::new(writer.into_cow(), format, max_read_size)
                    },
                );
                test_records_max_size_boundary(
                    format,
                    BufferRecordWriter::new,
                    |writer, format, max_read_size| {
                        BufferRecordReader::new(writer.into_cow(), format, max_read_size)
                    },
                );
            }
            Format::Chunk => {}
        }
    }

    #[test]
    fn buffer_buffer_records() {
        buffer_buffer(Format::Record)
    }

    #[test]
    fn buffer_buffer_chunks() {
        buffer_buffer(Format::Chunk)
    }

    #[test]
    fn buffer_buffer_records32() {
        buffer_buffer(Format::Record32)
    }

    #[test]
    fn into_owned_preserves_unread_records() {
        let mut w = BufferRecordWriter::new(Format::Record);
        w.write_record(b"one").unwrap();
        w.write_record(b"two").unwrap();
        let records: Vec<u8> = w.into();

        let mut rr =
            BufferRecordReader::new(std::borrow::Cow::Borrowed(&records), Format::Record, 16);
        assert_eq!(rr.read_record().unwrap(), b"one");

        let mut owned = rr.into_owned();
        drop(records);
        assert_eq!(owned.read_record().unwrap(), b"two");
        assert!(owned.maybe_read_record().unwrap().is_none());
    }

    fn stops_at_first_oversized_record(format: Format) {
        let mut w = BufferRecordWriter::new(format);
        w.write_record(b"toolong").unwrap();
        w.write_record(b"ok").unwrap();

        let mut rr = BufferRecordReader::new(w.into_cow(), format, 3);
        assert!(rr.maybe_read_record().is_err());
        assert!(rr.maybe_read_record().is_err());
    }

    #[test]
    fn record_stops_at_first_oversized_record() {
        stops_at_first_oversized_record(Format::Record);
    }

    #[test]
    fn record32_stops_at_first_oversized_record() {
        stops_at_first_oversized_record(Format::Record32);
    }
}
