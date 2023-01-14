use crc::{Crc, Digest as CrcDigest, CRC_32_CKSUM};
use sha2::{Digest, Sha512};
use std::collections::HashSet;
use std::io::{LineWriter, Write};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;

pub struct ValidationState<'a> {
    crc_digest: CrcDigest<'a, u32>,
    sha2_digest: Sha512,
    byte_count: usize,
    line_count: usize,
    seen_ids: HashSet<u64>,
}

impl<'a> ValidationState<'a> {
    /*pub fn new() -> LineWriter<ValidationState<'a>> {
        LineWriter::new(ValidationState {
            crc_digest: Crc::<u32>::new(&CRC_32_CKSUM).digest(),
            sha2_digest: Sha512::new(),
            byte_count: 0,
            line_count: 0,
            seen_ids: HashSet::new(),
        })
    }*/
}

impl<'a> AsyncWrite for ValidationState<'a> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        todo![]
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        todo![]
    }
}
