use std::error::Error;
use crate::rows::{Row, Columns};
use sqlparser::ast::{Expr, Ident, Value};

pub type Getter = Box<dyn Fn(&Row) -> String>;

#[derive(Debug, Default)]
struct GetterBuilder {
    columns: Columns,
    expr: Option<Expr>,
}

impl GetterBuilder {
    pub fn new() -> Self {
        GetterBuilder::default()
    }


    pub fn with_columns(mut self, columns: &Columns) -> Self {
        self.columns = columns.clone();
        self
    }

    pub fn with_expr(mut self, expr: &Option<Expr>) -> Self {
        self.expr = expr.clone();
        self
    }


    pub fn build(&self) -> Result<Getter, Box<dyn Error>> {
        let res = self.generate(self.expr.clone())?;
        Ok(res)
    }

    pub fn generate(&self, x: Option<Expr>) -> Result<Getter, Box<dyn Error>> {
        match x {
            Some(x) => self.generate_expr(x),
            None => Ok(self.generate_default()),
        }
    }

    pub fn generate_expr(&self, x: Expr) -> Result<Getter, Box<dyn Error>> {
        match x {
            Expr::Identifier(x) => self.generate_identifier(x),
            Expr::Value(v) => self.generate_value(v),
            _ => Err(format!("cannot generate getter from {:?}", x).into()),
        }
    }

    fn generate_identifier(&self, x: Ident) -> Result<Getter, Box<dyn Error>> {
        self.generate_ident(x)
    }

    fn generate_value(&self, v: Value) -> Result<Getter, Box<dyn Error>> {
        match v {
            Value::SingleQuotedString(s) => Ok(self.generate_from_str(s)),
            Value::Number(n, _) => Ok(self.generate_from_str(format!("{}", n).to_string())),
            _ => Err(format!("cannot generate getter from {:?}", v).into()),
        }
    }

    fn generate_ident(&self, x: Ident) -> Result<Getter, Box<dyn Error>> {
        let pos = self.columns.iter().position(|c| c == &x.value);
        match pos {
            Some(pos) => Ok(Box::new(move |row: &Row| -> String { row[pos].clone() })),
            None => Err(format!("column \"{}\" is not found", x.value).into()),
        }
    }

    fn generate_from_str(&self, s: String) -> Getter {
        Box::new(move |_row: &Row| -> String { s.clone() })
    }

    fn generate_default(&self) -> Getter {
        Box::new(|_row: &Row| -> String { "".into() })
    }

}

pub fn gen_from(columns: &Columns, expr: &Option<Expr>) -> Result<Getter, Box<dyn Error>> {
    GetterBuilder::new().with_columns(columns).with_expr(expr).build()
}

pub fn gen_from_index(index: usize) -> Getter {
    Box::new(move |row: &Row| -> String { row[index].clone() })
}


#[cfg(test)]
mod tests {
    use sqlparser::ast::Ident;

    use super::*;
    use crate::rows::from_str;

    #[test]
    fn test_getter_builder() {
        let columns: Columns = from_str(vec!["col1", "col2", "col3"]);
        let expr = Expr::Identifier(Ident::new("col1"));
        let getter = GetterBuilder::new()
            .with_columns(&columns)
            .with_expr(&Some(expr))
            .build()
            .unwrap();

        let row = from_str(vec!["1", "2", "3"]);
        assert_eq!(getter(&row), "1");
    }
}


