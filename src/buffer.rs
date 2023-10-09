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
    fn write_record<'a>(&'a mut self, data: &[u8]) -> Result<()> {
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
        BufferRecordWriter {
            records: Vec::default(),
            format,
        }
    }

    pub fn into_cow(self) -> Cow<'static, [u8]> {
        self.records.into()
    }
}

impl Into<Vec<u8>> for BufferRecordWriter {
    fn into(self) -> Vec<u8> {
        self.records
    }
}

// BufferRecordReader

impl RecordReader for BufferRecordReader<'_> {
    fn maybe_read_record<'a>(&'a mut self) -> Result<Option<&'a [u8]>> {
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
    ) -> BufferRecordReader {
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
            |format| BufferRecordWriter::new(format),
            |writer, format, max_read_size| {
                BufferRecordReader::new(writer.into_cow(), format, max_read_size)
            },
        );
    }

    #[test]
    fn buffer_buffer_records() {
        buffer_buffer(Format::Record)
    }

    #[test]
    fn buffer_buffer_chunks() {
        buffer_buffer(Format::Chunk)
    }
}
