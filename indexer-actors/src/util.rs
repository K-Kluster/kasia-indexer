pub trait ToHex: AsRef<[u8]> {
    fn to_hex(&self) -> String {
        faster_hex::hex_string(self.as_ref())
    }
}

impl<T: AsRef<[u8]>> ToHex for T {}
