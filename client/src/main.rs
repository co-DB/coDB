fn main() {
    println!("Hello client!");
}

#[cfg(test)]
mod tests {
    #[test]
    fn dummy_test() {
        assert_eq!(2 + 3, 5);
    }
}
