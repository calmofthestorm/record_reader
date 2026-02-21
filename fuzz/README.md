# Fuzzing

This project uses `cargo-fuzz` for parser hardening.

## Install

```bash
cargo install cargo-fuzz
```

## Run

```bash
cargo fuzz run maybe_read_record_from_buffer
cargo fuzz run stateful_offset_size
cargo fuzz run io_record_reader_from_read
cargo fuzz run roundtrip_buffer
cargo fuzz run io_roundtrip
cargo fuzz run cross_impl_equivalence
cargo fuzz run hashing_wrapper_consistency
cargo fuzz run format_boundary_cases
```
