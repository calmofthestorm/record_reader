use std::convert::TryInto;
use std::io::Write;

use anyhow::{Context, Result};
use byteorder::{ByteOrder, NetworkEndian};

use crate::*;

// This is public to allow other record readers to use the same logic as
// BufferRecordReader to read from a buffer without the lifetime issues of
// self-referential structs. ouroboros can do this, but it's not worth the
// complexity, dependencies, boxing, etc for this.
pub fn maybe_read_record_from_buffer<'a>(
    buf: &'a [u8],
    offset: &mut usize,
    size: usize,
    max_record_size: usize,
    format: Format,
) -> Result<Option<&'a [u8]>> {
    if *offset >= size {
        return Ok(None);
    }
    let data = &buf[*offset..];

    match format {
        Format::Record => {
            let length = NetworkEndian::read_u64(&data[..8]) as usize;
            if length > max_record_size {
                return None.context("incomplete record (or buffer is too small)");
            }
            *offset += 8 + length;
            Ok(Some(&data[8..8 + length]))
        }
        Format::Record32 => {
            let length = NetworkEndian::read_u32(&data[..4]) as usize;
            if length > max_record_size {
                return None.context("incomplete record (or buffer is too small)");
            }
            *offset += 4 + length;
            Ok(Some(&data[4..4 + length]))
        }
        Format::Chunk => {
            let length = std::cmp::min(max_record_size, data.len());
            *offset += length;
            Ok(Some(&data[..length]))
        }
    }
}

// As with `maybe_read_record_from_buffer`, this is public to allow conceptual
// composition without the hassle.
pub fn write_record<F>(stream: &mut F, data: &[u8]) -> Result<()>
where
    F: Write,
{
    let mut buf = [0; 8];
    NetworkEndian::write_u64(&mut buf, data.len().try_into().context("data too long")?);
    stream.write_all(&buf).context("write length")?;
    stream.write_all(data).context("write data")?;
    Ok(())
}

pub fn write_record32<F>(stream: &mut F, data: &[u8]) -> Result<()>
where
    F: Write,
{
    let mut buf = [0; 4];
    NetworkEndian::write_u32(&mut buf, data.len().try_into().context("data too long")?);
    stream.write_all(&buf).context("write length")?;
    stream.write_all(data).context("write data")?;
    Ok(())
}
