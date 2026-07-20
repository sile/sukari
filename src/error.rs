use std::io;

pub(crate) fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

pub(crate) fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub(crate) fn invalid_json(error: nojson::JsonParseError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

pub(crate) fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).expect("usize value should fit in u64")
}
