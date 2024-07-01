// Row are the datum that from the source
pub type Row = Vec<String>;

// RowIter iterator in row
pub type RowIter = Box<dyn Iterator<Item = Row>>;

// Header are the first row of the source
pub type Columns = Row;

pub fn from_str(v: Vec<&str>) -> Row {
    v.iter().map(|s| s.to_string()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_from_iter() {
        let row: Row = from_str(vec!["a", "b", "c"]);
        assert_eq!(row, vec!["a", "b", "c"]);
    }
}
