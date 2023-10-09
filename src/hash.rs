use anyhow::Result;
use digest::Digest;

use crate::*;

// HashingRecordReader

/// Hashes all records concatenated together into a single hash.
pub struct HashingRecordReader<I: RecordReader, D: Digest> {
    inner: I,
    hasher: D,
}

impl<I: RecordReader, D: Digest> HashingRecordReader<I, D> {
    pub fn new(inner: I, hasher: D) -> Result<HashingRecordReader<I, D>> {
        Ok(HashingRecordReader { inner, hasher })
    }
}

impl<I: RecordReader, D: Digest> RecordReader for HashingRecordReader<I, D> {
    fn maybe_read_record<'a>(&'a mut self) -> Result<Option<&'a [u8]>> {
        let record = self.inner.maybe_read_record()?;
        if let Some(data) = record {
            self.hasher.update(data);
        }
        Ok(record)
    }
}

// HashingRecordWriter

/// Hashes all records concatenated together into a single hash.
pub struct HashingRecordWriter<O: RecordWriter, D: Digest> {
    inner: O,
    hasher: D,
}

impl<O: RecordWriter, D: Digest> HashingRecordWriter<O, D> {
    pub fn new(inner: O, hasher: D) -> Result<HashingRecordWriter<O, D>> {
        Ok(HashingRecordWriter { inner, hasher })
    }
}

impl<O: RecordWriter, D: Digest> RecordWriter for HashingRecordWriter<O, D> {
    fn write_record<'a>(&'a mut self, data: &[u8]) -> Result<()> {
        self.inner.write_record(data)?;
        self.hasher.update(data);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.inner.flush().context("flush HashingRecordWriter")
    }
}
