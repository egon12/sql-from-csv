use crate::{rows::Columns, getter::{self, Getter}};
use sqlparser::ast::SelectItem;
use std::error::Error;


pub type Mapper = Vec<Getter>;

#[derive(Debug, Default)]
pub struct MapperBuilder {
    columns: Columns,
    selection: Vec<SelectItem>,
}

impl MapperBuilder {
    pub fn new() -> Self {
        MapperBuilder::default()
    }

    pub fn with_columns(mut self, columns: &Columns) -> Self {
        self.columns = columns.clone();
        self
    }

    pub fn with_selection(mut self, selection: &Vec<SelectItem>) -> Self {
        self.selection = selection.clone();
        self
    }

    pub fn build(&self) -> Result<Mapper, Box<dyn Error>> {
        self.generate(self.selection.clone())
    }

    pub fn generate(&self, s: Vec<SelectItem>) -> Result<Mapper, Box<dyn Error>> {
        if s.len() == 1 {
            if let SelectItem::Wildcard(_) = s[0] {
                return Ok(self.columns
                    .iter()
                    .enumerate()
                    .map(|(i, _)| -> Getter { getter::gen_from_index(i) })
                    .collect::<Vec<Getter>>());
            }
        }
        s.iter().map(|x| self.generate_select_item(x.clone())).collect()
    }

    pub fn generate_select_item(&self, s: SelectItem) -> Result<Getter, Box<dyn Error>> {
        match s {
            SelectItem::UnnamedExpr(x) => getter::gen_from(&self.columns, &Some(x)),
            _ => Err("unsupported expression in where clause".into()),
        }
    }
}

pub fn gen_from(columns: &Columns, selection: &Vec<SelectItem>) -> Result<Mapper, Box<dyn Error>> {
    MapperBuilder::new().with_columns(columns).with_selection(selection).build()
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_filter_builder() {
    }
}


