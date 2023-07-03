use crate::parser::Literal;
use crate::row::RowCol;

// the max amount of a varchar that is used in the key
const MAX_KEY_VAR_CHAR_LEN: usize = 128;

pub trait KeyPart {
    fn to_key_part(&self, key: &mut Vec<u8>);
}

// generate a byte string that represents the the key of a row
pub fn build<KP: KeyPart>(parts: &[KP]) -> Vec<u8> {
    let mut key = Vec::with_capacity(parts.len());

    // this is probably not the most efficient way to do this
    for part in parts {
        part.to_key_part(&mut key);

        // insert a separator _ between values, the last one will be removed before we return
        key.push(b'_');
    }

    // pop off the last _ that was inserted
    key.pop();
    key
}

impl KeyPart for RowCol {
    fn to_key_part(&self, key: &mut Vec<u8>) {
        (&self).to_key_part(key)
    }
}

impl KeyPart for &RowCol {
    fn to_key_part(&self, key: &mut Vec<u8>) {
        match self {
            RowCol::Int(i) => key.extend(i.to_be_bytes()),
            RowCol::Bool(b) => key.push(if *b { 1 } else { 0 }),
            RowCol::VarChar(vc) => {
                key.extend(&vc.inline[..vc.inline.len().min(MAX_KEY_VAR_CHAR_LEN)]);
            }
        }
    }
}

impl KeyPart for &Literal {
    fn to_key_part(&self, key: &mut Vec<u8>) {
        match self {
            Literal::Int(i) => key.extend(i.to_be_bytes()),
            Literal::Bool(b) => key.push(if *b { 1 } else { 0 }),
            Literal::String(s) => {
                let s = s.as_bytes();
                key.extend(&s[..s.len().min(MAX_KEY_VAR_CHAR_LEN)]);
            }
        }
    }
}
