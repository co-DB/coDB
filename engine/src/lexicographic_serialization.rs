use crate::data_types::{DbDate, DbDateTime};
use std::array::TryFromSliceError;
use std::string::FromUtf8Error;
use thiserror::Error;

#[derive(Error, Debug)]
enum DecodeError {
    #[error("invalid length of the bytes: expected {expected}, got {got}")]
    InvalidLength { expected: usize, got: usize },
    #[error("bytes don't contain valid utf-8 string: {0}")]
    InvalidUtf8(#[from] FromUtf8Error),
    #[error("error occurred with slice conversion: {0}")]
    SliceConversion(#[from] TryFromSliceError),
}

trait SortableSerialize: Sized {
    fn encode_key(self) -> Vec<u8>;
    fn decode_key(bytes: &[u8]) -> Result<Self, DecodeError>;
}

impl SortableSerialize for DbDate {
    fn encode_key(self) -> Vec<u8> {
        // Apply sign-bit flip like i32 to make it sortable
        let repr = (self.days_since_epoch() as u32) ^ 0x80000000;
        Vec::from(repr.to_be_bytes())
    }

    fn decode_key(bytes: &[u8]) -> Result<Self, DecodeError> {
        if bytes.len() < 4 {
            return Err(DecodeError::InvalidLength {
                expected: 4,
                got: bytes.len(),
            });
        }
        let bits = u32::from_be_bytes(bytes[..4].try_into()?);
        let days = (bits ^ 0x80000000) as i32;
        Ok(DbDate::new(days))
    }
}

impl SortableSerialize for DbDateTime {
    fn encode_key(self) -> Vec<u8> {
        let mut buffer = Vec::with_capacity(8);
        buffer.extend(self.days_since_epoch().encode_key());
        buffer.extend(self.milliseconds_since_midnight().to_be_bytes());
        buffer
    }

    fn decode_key(bytes: &[u8]) -> Result<Self, DecodeError> {
        if bytes.len() < 8 {
            return Err(DecodeError::InvalidLength {
                expected: 8,
                got: bytes.len(),
            });
        }
        let date = DbDate::decode_key(&bytes[..4])?;
        let millis = u32::from_be_bytes(bytes[4..8].try_into()?);
        Ok(DbDateTime::new(date, millis))
    }
}

impl SortableSerialize for i32 {
    fn encode_key(self) -> Vec<u8> {
        let repr = (self as u32) ^ 0x80000000;
        Vec::from(repr.to_be_bytes())
    }

    fn decode_key(bytes: &[u8]) -> Result<Self, DecodeError> {
        if bytes.len() < 4 {
            return Err(DecodeError::InvalidLength {
                expected: 4,
                got: bytes.len(),
            });
        }
        let bits = u32::from_be_bytes(bytes[..4].try_into()?);
        Ok((bits ^ 0x80000000) as i32)
    }
}

impl SortableSerialize for i64 {
    fn encode_key(self) -> Vec<u8> {
        let repr = (self as u64) ^ 0x8000000000000000;
        Vec::from(repr.to_be_bytes())
    }

    fn decode_key(bytes: &[u8]) -> Result<Self, DecodeError> {
        if bytes.len() < 8 {
            return Err(DecodeError::InvalidLength {
                expected: 8,
                got: bytes.len(),
            });
        }
        let bits = u64::from_be_bytes(bytes[..8].try_into()?);
        Ok((bits ^ 0x8000000000000000) as i64)
    }
}

impl SortableSerialize for String {
    fn encode_key(self) -> Vec<u8> {
        self.into_bytes()
    }

    fn decode_key(bytes: &[u8]) -> Result<Self, DecodeError> {
        Ok(String::from_utf8(bytes.to_vec())?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_i32_ordering() {
        let values = vec![i32::MIN, -1000000, -1000, -1, 0, 1, 1000, 1000000, i32::MAX];

        let mut encoded: Vec<_> = values.iter().map(|&v| (v, v.encode_key())).collect();

        // Sort by encoded bytes
        encoded.sort_by(|a, b| a.1.cmp(&b.1));

        // Extract the sorted values
        let sorted_values: Vec<_> = encoded.iter().map(|(v, _)| *v).collect();

        // Should match the original order
        assert_eq!(
            sorted_values, values,
            "i32 values should sort correctly by encoded bytes"
        );

        // Verify all values round-trip correctly
        for (original, bytes) in &encoded {
            let decoded = i32::decode_key(bytes).unwrap();
            assert_eq!(*original, decoded, "i32 should round-trip correctly");
        }
    }

    #[test]
    fn test_i64_ordering() {
        let values = vec![
            i64::MIN,
            -1_000_000_000_000,
            -1000,
            -1,
            0,
            1,
            1000,
            1_000_000_000_000,
            i64::MAX,
        ];

        let mut encoded: Vec<_> = values.iter().map(|&v| (v, v.encode_key())).collect();
        encoded.sort_by(|a, b| a.1.cmp(&b.1));
        let sorted_values: Vec<_> = encoded.iter().map(|(v, _)| *v).collect();

        assert_eq!(
            sorted_values, values,
            "i64 values should sort correctly by encoded bytes"
        );

        for (original, bytes) in &encoded {
            let decoded = i64::decode_key(bytes).unwrap();
            assert_eq!(*original, decoded, "i64 should round-trip correctly");
        }
    }

    #[test]
    fn test_string_ordering() {
        let values = vec![
            "", "a", "aa", "ab", "b", "hello", "world", "🦀", // Unicode support
        ];

        let mut encoded: Vec<_> = values
            .iter()
            .map(|&v| (v, v.to_string().encode_key()))
            .collect();

        encoded.sort_by(|a, b| a.1.cmp(&b.1));
        let sorted_values: Vec<_> = encoded.iter().map(|(v, _)| *v).collect();

        assert_eq!(
            sorted_values, values,
            "Strings should sort correctly by encoded bytes"
        );

        for (original, bytes) in &encoded {
            let decoded = String::decode_key(bytes).unwrap();
            assert_eq!(*original, decoded, "String should round-trip correctly");
        }
    }

    #[test]
    fn test_dbdate_ordering() {
        let values = vec![
            DbDate::new(i32::MIN),
            DbDate::new(-1000),
            DbDate::new(-1),
            DbDate::new(0),
            DbDate::new(1),
            DbDate::new(1000),
            DbDate::new(i32::MAX),
        ];

        let mut encoded: Vec<_> = values.iter().map(|&v| (v, v.encode_key())).collect();
        encoded.sort_by(|a, b| a.1.cmp(&b.1));
        let sorted_values: Vec<_> = encoded.iter().map(|(v, _)| *v).collect();

        assert_eq!(
            sorted_values, values,
            "DbDate values should sort correctly by encoded bytes"
        );

        for (original, bytes) in &encoded {
            let decoded = DbDate::decode_key(bytes).unwrap();
            assert_eq!(*original, decoded, "DbDate should round-trip correctly");
        }
    }

    #[test]
    fn test_dbdatetime_ordering() {
        let values = vec![
            DbDateTime::new(DbDate::new(i32::MIN), 0),
            DbDateTime::new(DbDate::new(-100), 0),
            DbDateTime::new(DbDate::new(-100), 500),
            DbDateTime::new(DbDate::new(-100), 86399999), // Last millisecond of day
            DbDateTime::new(DbDate::new(0), 0),
            DbDateTime::new(DbDate::new(0), 1000),
            DbDateTime::new(DbDate::new(1), 0),
            DbDateTime::new(DbDate::new(100), 0),
            DbDateTime::new(DbDate::new(100), 86399999),
            DbDateTime::new(DbDate::new(i32::MAX), 86399999),
        ];

        let mut encoded: Vec<_> = values.iter().map(|&v| (v, v.encode_key())).collect();
        encoded.sort_by(|a, b| a.1.cmp(&b.1));
        let sorted_values: Vec<_> = encoded.iter().map(|(v, _)| *v).collect();

        assert_eq!(
            sorted_values, values,
            "DbDateTime values should sort correctly by encoded bytes"
        );

        for (original, bytes) in &encoded {
            let decoded = DbDateTime::decode_key(bytes).unwrap();
            assert_eq!(*original, decoded, "DbDateTime should round-trip correctly");
        }
    }

    #[test]
    fn test_cross_type_consistency() {
        // Test that the same underlying i32 value in DbDate sorts the same way as a standalone i32
        let i32_values = [-100, -1, 0, 1, 100];
        let date_values: Vec<_> = i32_values.iter().map(|&v| DbDate::new(v)).collect();

        let mut i32_encoded: Vec<_> = i32_values.iter().map(|&v| v.encode_key()).collect();
        let mut date_encoded: Vec<_> = date_values.iter().map(|v| v.encode_key()).collect();

        i32_encoded.sort();
        date_encoded.sort();

        // The encoded bytes for the days portion should have the same ordering
        for i in 0..i32_values.len() {
            assert_eq!(
                i32_encoded[i], date_encoded[i],
                "i32 and DbDate encodings should match for value {}",
                i32_values[i]
            );
        }
    }

    #[test]
    fn test_decode_errors() {
        // Test various error conditions

        // Too short for i32
        assert!(i32::decode_key(&[0, 0, 0]).is_err());

        // Too short for i64
        assert!(i64::decode_key(&[0, 0, 0, 0]).is_err());

        // Invalid UTF-8
        let mut invalid_utf8 = vec![0, 0, 0, 3];
        invalid_utf8.extend_from_slice(&[0xFF, 0xFF, 0xFF]); // Invalid UTF-8
        assert!(String::decode_key(&invalid_utf8).is_err());

        // Too short for DbDate
        assert!(DbDate::decode_key(&[0, 0, 0]).is_err());

        // Too short for DbDateTime
        assert!(DbDateTime::decode_key(&[0, 0, 0, 0, 0, 0, 0]).is_err());
    }

    #[test]
    fn test_empty_string() {
        let empty = String::new();
        let encoded = empty.encode_key();
        let decoded = String::decode_key(&encoded).unwrap();
        assert_eq!(String::new(), decoded);
        assert_eq!(encoded.len(), 0);
    }

    #[test]
    fn test_boundary_values() {
        let boundaries = vec![
            (i32::MIN, "i32::MIN"),
            (i32::MIN + 1, "i32::MIN + 1"),
            (-1, "-1"),
            (0, "0"),
            (1, "1"),
            (i32::MAX - 1, "i32::MAX - 1"),
            (i32::MAX, "i32::MAX"),
        ];

        for (value, label) in boundaries {
            let encoded = value.encode_key();
            let decoded = i32::decode_key(&encoded).unwrap();
            assert_eq!(value, decoded, "Boundary value {} should round-trip", label);
        }
    }
}
