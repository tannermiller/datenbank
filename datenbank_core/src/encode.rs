use std::io::Write;

pub fn write_len(buffer: &mut Vec<u8>, len: usize) {
    buffer
        .write_all(&(len as u16).to_be_bytes())
        .expect("can't fail writing to vec");
}

pub fn write_len_and_bytes(buffer: &mut Vec<u8>, bytes: &[u8]) {
    write_len(buffer, bytes.len());
    buffer.write_all(bytes).expect("can't fail writing to vec");
}
