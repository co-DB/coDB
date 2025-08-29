pub mod background_worker;
pub mod cache;
pub mod catalog;
mod data_types;
pub mod files_manager;
pub mod paged_file;
pub mod record;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
