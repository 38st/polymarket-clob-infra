//! Minimal Ethereum ABI helpers over Alloy, shaped like the ethers-rs API the
//! receipt, settlement and chain code was written against. Encodings are
//! Alloy's; the fixed receipt and signature vectors in the tests pin them.
use alloy_dyn_abi::{DynSolType, DynSolValue};
pub use alloy_primitives::{Address, U256};
use anyhow::{ensure, Context, Result};

pub fn keccak256(data: impl AsRef<[u8]>) -> [u8; 32] {
    alloy_primitives::keccak256(data).0
}

/// Strict decimal integer (token IDs are decimal; hex is rejected).
pub fn u256_dec(value: &str) -> Result<U256> {
    ensure!(
        !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()),
        "Invalid decimal integer"
    );
    U256::from_str_radix(value, 10).map_err(|_| anyhow::anyhow!("Integer exceeds 256 bits"))
}

/// Big-endian integer of at most 32 bytes.
pub fn u256_be(bytes: &[u8]) -> U256 {
    U256::from_be_slice(bytes)
}

/// 64 lowercase hex digits (one ABI word), without a prefix.
#[cfg(test)]
pub fn word_hex(value: U256) -> String {
    hex::encode(value.to_be_bytes::<32>())
}

/// An address returned as a 256-bit word, as lowercase `0x` hex.
pub fn address_hex(value: U256) -> Result<String> {
    ensure!(value.bit_len() <= 160, "Invalid address word");
    Ok(format!("0x{}", hex::encode(&value.to_be_bytes::<32>()[12..])))
}

/// Lowercase `0x` hex of an address (Alloy's Display is checksummed).
pub fn address_lower(address: &Address) -> String {
    format!("0x{}", hex::encode(address.as_slice()))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Token {
    Uint(U256),
    Address(Address),
    /// At most 32 bytes, left-aligned in its word.
    FixedBytes(Vec<u8>),
    Array(Vec<Token>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParamType {
    Uint(usize),
    FixedBytes(usize),
    Array(Box<ParamType>),
}

impl Token {
    fn to_dyn(&self) -> DynSolValue {
        match self {
            Token::Uint(value) => DynSolValue::Uint(*value, 256),
            Token::Address(address) => DynSolValue::Address(*address),
            Token::FixedBytes(bytes) => {
                assert!(bytes.len() <= 32, "fixed bytes exceed one word");
                let mut word = [0u8; 32];
                word[..bytes.len()].copy_from_slice(bytes);
                DynSolValue::FixedBytes(word.into(), bytes.len())
            }
            Token::Array(items) => DynSolValue::Array(items.iter().map(Token::to_dyn).collect()),
        }
    }
    fn from_dyn(value: DynSolValue) -> Result<Token> {
        Ok(match value {
            DynSolValue::Uint(value, _) => Token::Uint(value),
            DynSolValue::Address(address) => Token::Address(address),
            DynSolValue::FixedBytes(word, size) => Token::FixedBytes(word[..size].to_vec()),
            DynSolValue::Array(items) => Token::Array(
                items
                    .into_iter()
                    .map(Token::from_dyn)
                    .collect::<Result<_>>()?,
            ),
            _ => anyhow::bail!("Unsupported ABI value"),
        })
    }
    pub fn into_uint(self) -> Option<U256> {
        match self {
            Token::Uint(value) => Some(value),
            _ => None,
        }
    }
    pub fn into_array(self) -> Option<Vec<Token>> {
        match self {
            Token::Array(items) => Some(items),
            _ => None,
        }
    }
    pub fn into_fixed_bytes(self) -> Option<Vec<u8>> {
        match self {
            Token::FixedBytes(bytes) => Some(bytes),
            _ => None,
        }
    }
}

impl ParamType {
    fn to_dyn(&self) -> DynSolType {
        match self {
            ParamType::Uint(bits) => DynSolType::Uint(*bits),
            ParamType::FixedBytes(size) => DynSolType::FixedBytes(*size),
            ParamType::Array(item) => DynSolType::Array(Box::new(item.to_dyn())),
        }
    }
}

/// Head/tail encoding of a parameter list (as for call data after the selector).
pub fn encode(tokens: &[Token]) -> Vec<u8> {
    DynSolValue::Tuple(tokens.iter().map(Token::to_dyn).collect()).abi_encode_params()
}

pub fn decode(types: &[ParamType], data: &[u8]) -> Result<Vec<Token>> {
    let value = DynSolType::Tuple(types.iter().map(ParamType::to_dyn).collect())
        .abi_decode_params(data)
        .context("Invalid ABI data")?;
    match value {
        DynSolValue::Tuple(items) => items.into_iter().map(Token::from_dyn).collect(),
        _ => anyhow::bail!("Invalid ABI data"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Integer overflow panics in every build of this crate, release too
    /// (Cargo.toml [profile.release]): `cargo test --release`
    /// fails here without it, where arithmetic would otherwise wrap silently.
    #[test]
    fn integer_overflow_panics_in_every_build() {
        let overflowed = std::panic::catch_unwind(|| std::hint::black_box(u8::MAX) + std::hint::black_box(1u8));
        assert!(overflowed.is_err(), "integer overflow wrapped: overflow checks are off");
    }
    #[test]
    fn encodings_match_the_solidity_abi() {
        // balanceOf(address,uint256) call data for a known account and token.
        let owner: Address = "0x1111111111111111111111111111111111111111".parse().unwrap();
        let mut call = keccak256("balanceOf(address,uint256)")[..4].to_vec();
        call.extend(encode(&[Token::Address(owner), Token::Uint(U256::from(123u64))]));
        assert_eq!(
            hex::encode(call),
            format!("00fdd58e{:0>64}{:064x}", "1111111111111111111111111111111111111111", 123)
        );
        // Dynamic arrays round-trip canonically and reject truncation.
        let tokens = vec![
            Token::FixedBytes(vec![0xab; 32]),
            Token::Array(vec![Token::Uint(U256::from(1u64)), Token::Uint(U256::from(2u64))]),
            Token::Uint(U256::from(7u64)),
        ];
        let types = [
            ParamType::FixedBytes(32),
            ParamType::Array(Box::new(ParamType::Uint(256))),
            ParamType::Uint(256),
        ];
        let bytes = encode(&tokens);
        assert_eq!(bytes.len(), 32 * 6);
        assert_eq!(decode(&types, &bytes).unwrap(), tokens);
        assert!(decode(&types, &bytes[..bytes.len() - 1]).is_err());
        assert_eq!(
            hex::encode(keccak256("")),
            "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
        );
        assert!(u256_dec("0x10").is_err() && u256_dec("").is_err());
        assert_eq!(u256_dec("12345678901234567890").unwrap().to_string(), "12345678901234567890");
        assert_eq!(address_hex(U256::from(0x44u64)).unwrap(), format!("0x{:040x}", 0x44));
        assert!(address_hex(U256::MAX).is_err());
    }
}
