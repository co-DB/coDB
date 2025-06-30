fn main() {
    println!("Hello server!");
    println!("Ouput from engine function: {}", engine::add(2, 3));
}

#[cfg(test)]
mod tests {
    #[test]
    fn dummy_test() {
        assert_eq!(2 + 3, 5);
    }
}
