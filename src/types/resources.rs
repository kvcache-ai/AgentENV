pub fn bytes_to_mib_ceil(bytes: u64) -> u32 {
    bytes.div_ceil(1 << 20).try_into().unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::bytes_to_mib_ceil;

    #[test]
    fn bytes_to_mib_ceil_rounds_up() {
        assert_eq!(bytes_to_mib_ceil(0), 0);
        assert_eq!(bytes_to_mib_ceil(1), 1);
        assert_eq!(bytes_to_mib_ceil(1 << 20), 1);
        assert_eq!(bytes_to_mib_ceil((1 << 20) + 1), 2);
    }
}
