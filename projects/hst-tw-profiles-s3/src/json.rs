use serde::de::Deserialize;
use std::io::{LineWriter, Write};

pub trait JsonSink<'a> {
    type Item: Deserialize<'a>;
    type Output;

    fn process(&mut self, item: Self::Item);
    fn finish(&mut self) -> Self::Output;

    fn feed(&mut self, line: &'a [u8]) -> Result<(), std::io::Error> {
        match serde_json::from_slice::<Self::Item>(line) {
            Ok(item) => Ok(self.process(item)),
            Err(error) => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, error)),
        }
    }
}

pub struct Counter(pub usize);

impl Write for Counter {
    fn write(&mut self, buffer: &[u8]) -> Result<usize, std::io::Error> {
        Self::feed(self, buffer).map(|_| buffer.len())
    }
    fn flush(&mut self) -> Result<(), std::io::Error> {
        Ok(())
    }
}

impl JsonSink<'_> for Counter {
    type Item = serde_json::Value;
    type Output = usize;

    fn process(&mut self, item: Self::Item) {
        self.0 += 1;
    }

    fn finish(&mut self) -> Self::Output {
        self.0
    }
}
