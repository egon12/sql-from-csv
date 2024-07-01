use crate::{rows::{Row, Columns}, getter::{self, Getter}};
use sqlparser::ast::{BinaryOperator, Expr};
use std::error::Error;

pub type Filter = Box<dyn Fn(&Row) -> bool>;

#[derive(Debug, Default)]
pub struct FilterBuilder {
    columns: Columns,
    selection: Option<Expr>,
}

impl FilterBuilder {
    pub fn new() -> Self {
        FilterBuilder::default()
    }

    pub fn with_columns(mut self, columns: &Columns) -> Self {
        self.columns = columns.clone();
        self
    }

    pub fn with_selection(mut self, selection: &Option<Expr>) -> Self {
        self.selection = selection.clone();
        self
    }

    pub fn build(&self) -> Result<Filter, Box<dyn Error>> {
        self.generate(self.selection.clone())
    }

    pub fn generate(&self, s: Option<Expr>) -> Result<Filter, Box<dyn Error>> {
        match s {
            None => Ok(all()),
            Some(expr) => Ok(self.generate_expr(expr)?),
        }
    }

    pub fn generate_expr(&self, s: Expr) -> Result<Filter, Box<dyn Error>> {
        match s {
            Expr::BinaryOp { left, op, right } => {
                if op == BinaryOperator::And {
                    let left: Filter = self.generate_expr(*left)?;
                    let right: Filter = self.generate_expr(*right)?;
                    let f = move |row: &Row| -> bool { left(row) && right(row) };
                    return Ok(Box::new(f));
                }
                let left = getter::gen_from(&self.columns, &Some(*left))?;
                let right = getter::gen_from(&self.columns, &Some(*right))?;

                match op {
                    BinaryOperator::Eq => self.generate_eq(left, right),
                    BinaryOperator::NotEq => self.generate_neq(left, right),
                    BinaryOperator::Gt => self.generate_gt(left, right),
                    BinaryOperator::Lt => self.generate_lt(left, right),
                    _ => return Err(format!("unsupported operator {:?}", op).into()),
                }
            }
            _ => Err("unsupported expression in where clause".into()),
        }
    }

    pub fn generate_eq(&self, left: Getter, right: Getter) -> Result<Filter, Box<dyn Error>> {
        let f = move |row: &Row| -> bool { left(row) == right(row) };
        Ok(Box::new(f))
    }

    pub fn generate_gt(&self, left: Getter, right: Getter) -> Result<Filter, Box<dyn Error>> {
        let f = move |row: &Row| -> bool { left(row) > right(row) };
        Ok(Box::new(f))
    }

    pub fn generate_lt(&self, left: Getter, right: Getter) -> Result<Filter, Box<dyn Error>> {
        let f = move |row: &Row| -> bool { left(row) < right(row) };
        Ok(Box::new(f))
    }

    pub fn generate_neq(&self, left: Getter, right: Getter) -> Result<Filter, Box<dyn Error>> {
        let f = move |row: &Row| -> bool { left(row) != right(row) };
        Ok(Box::new(f))
    }
}

pub fn all() -> Filter {
    Box::new(|_row: &Row| -> bool { true })
}

pub fn gen_from(columns: &Columns, selection: &Option<Expr>) -> Result<Filter, Box<dyn Error>> {
    FilterBuilder::new().with_columns(columns).with_selection(selection).build()
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::Ident;

    use super::*;
    use crate::rows::from_str;
    use crate::rows::Columns;

    #[test]
    fn test_filter_builder() {
        let columns: Columns = from_str(vec!["col1", "col2", "col3"]);
        let left = Expr::Identifier(Ident::new("col1"));
        let right = Expr::Identifier(Ident::with_quote('\"', "1"));
        let selection = Expr::BinaryOp{
            left: Box::new(left),
            right: Box::new(right),
            op: BinaryOperator::Eq,
        };
        let filter = FilterBuilder::new()
            .with_columns(&columns)
            .with_selection(&Some(selection))
            .build()
            .unwrap();

        let row = from_str(vec!["1", "2", "3"]);
        assert_eq!(filter(&row), true);
    }
}


