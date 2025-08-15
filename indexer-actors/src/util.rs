#[allow(dead_code)]
#[allow(private_bounds)]
pub trait ToHex {
    fn to_hex64(&self) -> [u8; 64]
    where
        Self: FixedSize32,
    {
        let mut out = [0u8; 64];
        faster_hex::hex_encode(self.as_ref(), &mut out).unwrap();
        out
    }
    fn to_hex(&self) -> String
    where
        Self: AsRef<[u8]>,
    {
        faster_hex::hex_string(self.as_ref())
    }
}

trait FixedSize32: AsRef<[u8]> {}
impl FixedSize32 for [u8; 32] {}
