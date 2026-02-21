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
    let size = std::cmp::min(size, buf.len());
    if *offset >= size {
        return Ok(None);
    }
    let data = &buf[*offset..size];

    match format {
        Format::Record => {
            if data.len() < 8 {
                anyhow::bail!("incomplete record header");
            }
            let length = NetworkEndian::read_u64(&data[..8]) as usize;
            if length > max_record_size {
                anyhow::bail!("incomplete record (or buffer is too small)");
            }
            if data.len() < 8 + length {
                anyhow::bail!("incomplete record");
            }
            *offset += 8 + length;
            Ok(Some(&data[8..8 + length]))
        }
        Format::Record32 => {
            if data.len() < 4 {
                anyhow::bail!("incomplete record header");
            }
            let length = NetworkEndian::read_u32(&data[..4]) as usize;
            if length > max_record_size {
                anyhow::bail!("incomplete record (or buffer is too small)");
            }
            if data.len() < 4 + length {
                anyhow::bail!("incomplete record");
            }
            *offset += 4 + length;
            Ok(Some(&data[4..4 + length]))
        }
        Format::Chunk => {
            let length = std::cmp::min(max_record_size, data.len());
            if length == 0 {
                anyhow::bail!("incomplete record (or buffer is too small)");
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn malformed_record_header_returns_err() {
        let mut offset = 0;
        let r = maybe_read_record_from_buffer(&[0, 0, 0, 0], &mut offset, 4, 10, Format::Record);
        assert!(r.is_err());
    }

    #[test]
    fn malformed_record_payload_returns_err() {
        let mut buf = vec![0_u8; 8 + 3];
        NetworkEndian::write_u64(&mut buf[..8], 5);
        let mut offset = 0;
        let r = maybe_read_record_from_buffer(&buf, &mut offset, buf.len(), 10, Format::Record);
        assert!(r.is_err());
    }

    #[test]
    fn malformed_record32_header_returns_err() {
        let mut offset = 0;
        let r = maybe_read_record_from_buffer(&[0, 0], &mut offset, 2, 10, Format::Record32);
        assert!(r.is_err());
    }

    #[test]
    fn malformed_record32_payload_returns_err() {
        let mut buf = vec![0_u8; 4 + 2];
        NetworkEndian::write_u32(&mut buf[..4], 3);
        let mut offset = 0;
        let r = maybe_read_record_from_buffer(&buf, &mut offset, buf.len(), 10, Format::Record32);
        assert!(r.is_err());
    }

    #[test]
    fn chunk_with_zero_max_size_returns_err() {
        let mut offset = 0;
        let r = maybe_read_record_from_buffer(b"abc", &mut offset, 3, 0, Format::Chunk);
        assert!(r.is_err());
    }

    #[test]
    fn write_record_serialization_format() {
        let mut out = Vec::new();
        write_record(&mut out, b"hello").unwrap();
        assert_eq!(NetworkEndian::read_u64(&out[..8]), 5);
        assert_eq!(&out[8..], b"hello");
    }

    #[test]
    fn write_record32_serialization_format() {
        let mut out = Vec::new();
        write_record32(&mut out, b"hi").unwrap();
        assert_eq!(NetworkEndian::read_u32(&out[..4]), 2);
        assert_eq!(&out[4..], b"hi");
    }

    #[test]
    fn regression_truncated_record32_header_does_not_panic() {
        let mut offset = 0;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            maybe_read_record_from_buffer(&[0_u8; 2], &mut offset, 2, 10, Format::Record32)
        }));

        assert!(result.is_ok(), "should return an error, not panic");
        assert!(result.unwrap().is_err());
    }
}
