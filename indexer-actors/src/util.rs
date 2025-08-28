use fstr::FStr;

pub trait ToHex64 {
    fn to_hex_64(&self) -> FStr<64>;
}
impl ToHex64 for [u8; 32] {
    fn to_hex_64(&self) -> FStr<64> {
        let mut out = [0u8; 64];
        faster_hex::hex_encode(self.as_slice(), &mut out).unwrap();
        FStr::from_inner(out).unwrap()
    }
}

pub trait ToHex: AsRef<[u8]> {
    fn to_hex(&self) -> String {
        faster_hex::hex_string(self.as_ref())
    }
}

impl<T: AsRef<[u8]>> ToHex for T {}
