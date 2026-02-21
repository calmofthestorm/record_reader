#![no_main]

use byteorder::{ByteOrder, NetworkEndian};
use libfuzzer_sys::fuzz_target;
use record_reader::{util::maybe_read_record_from_buffer, Format};

fn run_case(format: Format, case: u8, len: usize, max_record_size: usize, pad: &[u8]) {
    let mut offset = 0usize;
    match (format, case % 5) {
        (Format::Record, 0) => {
            let buf = vec![0_u8; len % 8];
            let r = maybe_read_record_from_buffer(
                &buf,
                &mut offset,
                buf.len(),
                max_record_size,
                format,
            );
            if buf.is_empty() {
                assert!(matches!(r, Ok(None)));
            } else {
                assert!(r.is_err());
            }
        }
        (Format::Record32, 0) => {
            let buf = vec![0_u8; len % 4];
            let r = maybe_read_record_from_buffer(
                &buf,
                &mut offset,
                buf.len(),
                max_record_size,
                format,
            );
            if buf.is_empty() {
                assert!(matches!(r, Ok(None)));
            } else {
                assert!(r.is_err());
            }
        }
        (Format::Record, 1) => {
            let mut buf = vec![0_u8; 8];
            NetworkEndian::write_u64(&mut buf[..8], max_record_size as u64);
            buf.extend_from_slice(&pad[..std::cmp::min(max_record_size, pad.len())]);
            let r = maybe_read_record_from_buffer(
                &buf,
                &mut offset,
                buf.len(),
                max_record_size,
                format,
            );
            if max_record_size <= pad.len() {
                assert!(matches!(r, Ok(Some(_))));
            } else {
                assert!(r.is_err());
            }
        }
        (Format::Record32, 1) => {
            let write_len = std::cmp::min(max_record_size, u32::MAX as usize);
            let mut buf = vec![0_u8; 4];
            NetworkEndian::write_u32(&mut buf[..4], write_len as u32);
            buf.extend_from_slice(&pad[..std::cmp::min(write_len, pad.len())]);
            let r = maybe_read_record_from_buffer(
                &buf,
                &mut offset,
                buf.len(),
                max_record_size,
                format,
            );
            if write_len <= pad.len() {
                assert!(matches!(r, Ok(Some(_))));
            } else {
                assert!(r.is_err());
            }
        }
        (Format::Record, 2) => {
            let mut buf = vec![0_u8; 8];
            NetworkEndian::write_u64(&mut buf[..8], (max_record_size.saturating_add(1)) as u64);
            assert!(maybe_read_record_from_buffer(
                &buf,
                &mut offset,
                buf.len(),
                max_record_size,
                format
            )
            .is_err());
        }
        (Format::Record32, 2) => {
            let mut buf = vec![0_u8; 4];
            let n = std::cmp::min(u32::MAX as usize, max_record_size.saturating_add(1));
            NetworkEndian::write_u32(&mut buf[..4], n as u32);
            assert!(maybe_read_record_from_buffer(
                &buf,
                &mut offset,
                buf.len(),
                max_record_size,
                format
            )
            .is_err());
        }
        (_, 3) => {
            let r = maybe_read_record_from_buffer(pad, &mut offset, 0, max_record_size, format);
            assert!(matches!(r, Ok(None) | Err(_)));
        }
        _ => {
            let _ =
                maybe_read_record_from_buffer(pad, &mut offset, pad.len(), max_record_size, format);
        }
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() < 4 {
        return;
    }

    let format = match data[0] % 3 {
        0 => Format::Chunk,
        1 => Format::Record,
        _ => Format::Record32,
    };
    let case = data[1];
    let len = usize::from(data[2]);
    let max_record_size = usize::from(data[3]);
    let pad = &data[4..];

    run_case(format, case, len, max_record_size, pad);
});
