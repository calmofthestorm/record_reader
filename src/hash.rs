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

    #[must_use]
    pub fn into_inner(self) -> I {
        self.inner
    }

    #[must_use]
    pub fn finalize(self) -> digest::Output<D> {
        self.hasher.finalize()
    }
}

impl<I: RecordReader, D: Digest> RecordReader for HashingRecordReader<I, D> {
    fn maybe_read_record(&mut self) -> Result<Option<&[u8]>> {
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

    #[must_use]
    pub fn into_inner(self) -> O {
        self.inner
    }

    #[must_use]
    pub fn finalize(self) -> digest::Output<D> {
        self.hasher.finalize()
    }
}

impl<O: RecordWriter, D: Digest> RecordWriter for HashingRecordWriter<O, D> {
    fn write_record(&mut self, data: &[u8]) -> Result<()> {
        self.inner.write_record(data)?;
        self.hasher.update(data);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.inner.flush().context("flush HashingRecordWriter")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::Sha256;

    #[test]
    fn hashing_record_reader_passthrough_and_hash_vector() {
        let mut inner = BufferRecordWriter::new(Format::Chunk);
        inner.write_record(b"ab").unwrap();
        inner.write_record(b"c").unwrap();
        let inner = BufferRecordReader::new(inner.into_cow(), Format::Chunk, 2);

        let mut rr = HashingRecordReader::new(inner, Sha256::new()).unwrap();
        assert_eq!(rr.maybe_read_record().unwrap().unwrap(), b"ab");
        assert_eq!(rr.maybe_read_record().unwrap().unwrap(), b"c");
        assert!(rr.maybe_read_record().unwrap().is_none());

        let digest = rr.hasher.finalize();
        let expected: [u8; 32] = [
            0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
            0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
            0xf2, 0x00, 0x15, 0xad,
        ];
        assert_eq!(digest.as_slice(), expected.as_slice());
    }

    #[test]
    fn hashing_record_writer_passthrough_and_hash_vector() {
        let inner = BufferRecordWriter::new(Format::Chunk);
        let mut rw = HashingRecordWriter::new(inner, Sha256::new()).unwrap();
        rw.write_record(b"a").unwrap();
        rw.write_record(b"bc").unwrap();
        rw.flush().unwrap();

        let digest = rw.hasher.finalize();
        let expected: [u8; 32] = [
            0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
            0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
            0xf2, 0x00, 0x15, 0xad,
        ];
        assert_eq!(digest.as_slice(), expected.as_slice());

        let bytes: Vec<u8> = rw.inner.into();
        assert_eq!(bytes, b"abc");
    }
}
