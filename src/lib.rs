// TODO: Better error type
// https://nick.groenen.me/posts/rust-error-handling/#libraries-versus-applications
use anyhow::{Context, Result};
use byteorder::{ByteOrder, NetworkEndian};

pub mod buffer;
pub mod errors;
pub mod file;
pub mod util;

pub use buffer::*;
pub use errors::*;
pub use file::*;

use util::*;

#[cfg(any(test, feature = "digest"))]
pub mod hash;

#[cfg(any(test, feature = "digest"))]
pub use hash::*;

pub mod test_util;

#[derive(Clone, Copy, Debug)]
pub enum Format {
    // Read/write the plain bytes from the file in chunks. Will not read
    // zero-length chunks.
    Chunk,

    // Records consist of an 8 byte network-endian length followed by the
    // payload.
    Record,

    // Records consist of a 4 byte network-endian length followed by the
    // payload.
    Record32,
}

/// Unlike an iterator, this only guarantees each returned slice is valid until
/// the next call to [maybe_]read_record.
pub trait RecordReader {
    fn maybe_read_record<'a>(&'a mut self) -> Result<Option<&'a [u8]>>;

    fn read_record<'a>(&'a mut self) -> Result<&'a [u8]> {
        self.maybe_read_record()?.context("empty")
    }
}

pub trait RecordWriter {
    fn write_record(&mut self, data: &[u8]) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
}
