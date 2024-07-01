use csv::{Reader, Result as CsvResult, StringRecord};
use sqlparser::ast::{Expr, Query, Select, SelectItem, SetExpr, Statement, TableWithJoins, Value, ObjectName};
use sqlparser::{dialect::GenericDialect, parser::Parser};
use std::{env, error::Error, fs::File, io::{stdout, Write}};

pub mod rows;
pub mod getter;
pub mod filter;
pub mod mapper;

use rows::{Row, RowIter};

fn main() {
    let args: Vec<String> = env::args().collect();
    query(&args[1], &mut stdout());
}

fn query<W: Write>(query: &str, writer: &mut W) {
    let ast = Parser::parse_sql(&GenericDialect, query);
    let s = ast.unwrap();
    handle(&s[0], writer);
}

fn handle<W: Write>(s: &Statement, writer: &mut W) {
    match s {
        Statement::Query(query) => handle_query(query.clone(), writer),
        Statement::ExplainTable { table_name, .. } => handle_explain(table_name.clone(), writer),
        _ => println!("cannot handle statement: {:?}", s)
    }
}

fn handle_query<W: Write>(query: Box<Query>, writer: &mut W) {
    let expr: SetExpr = *query.body.clone();
    if let SetExpr::Select(select) = expr {
        match execute_select(*select, *query.clone()) {
            Ok(s) => s.for_each(|row| writeln!(writer, "{:?}", row).unwrap()),
            Err(e) => println!("Error: {:?}", e),
        };
    };
}

fn handle_explain<W: Write>(n: ObjectName, writer: &mut W) {

    let name = n;
    let mut rdr = Reader::from_path(name.to_string()).expect("Cannot find table".into());
    let h = rdr.headers().expect("Cannot extrac headers");
    let h: Vec<String> = h.iter().map(|s| s.to_string()).collect();
    let _ = h.iter().for_each(|i| {
        _ = writeln!(writer, "{:?}", i);
    });
}



fn execute_select(s: Select, q: Query) -> Result<RowIter, Box<dyn std::error::Error>> {
    let table = s
        .from
        .first()
        .ok_or("can't find \"from\" in select query")?;

    let mut qr = QueryResult::from_table(table)?;
    qr.set_select(&s)?;

    let filter = filter::gen_from(&qr.columns, &s.selection)?;
    let map = mapper::gen_from(&qr.columns, &s.projection)?;
    let source: RowIter = qr;

    let mut limit = 1000;
    if let Some(lim) = &q.limit {
        limit = extract_limit(lim.clone())?;
    }

    let res = source
        .filter(filter)
        //.fold(fold)
        .take(limit)
        .map(move |r| map.iter().map(|m| m(&r)).collect());

    Ok(Box::new(res))
}

fn extract_limit(limit: Expr) -> Result<usize, Box<dyn Error>> {
    match limit {
        Expr::Value(Value::Number(n, _)) => Ok(n.parse::<usize>()?),
        _ => Err("cannot evaluate complex expression on limit".into()),
    }
}

struct QueryResult {
    columns: Vec<String>,
    column_numbers: Vec<usize>,
    reader: Reader<File>,
}

impl QueryResult {

    fn from_table(table: &TableWithJoins) -> Result<Box<QueryResult>, Box<dyn Error>> {
        let mut reader: Reader<File> = Reader::from_path(table.relation.to_string())?;

        let columns = Self::extract_string_record(reader.headers()?);

        let result = QueryResult {
            columns,
            column_numbers: Vec::new(),
            reader,
        };

        Ok(Box::new(result))
    }

    fn set_select(&mut self, s: &Select) -> Result<(), Box<dyn Error>> {
        self.column_numbers = self.get_column_numbers(s.projection.clone())?;
        Ok(())
    }

    fn next_row(&mut self) -> Option<Vec<String>> {
        match self.reader.records().next() {
            Some(record) => Some(self.process_row(record)),
            None => None,
        }
    }

    fn process_row(&self, sr: CsvResult<StringRecord>) -> Row {
        if let Err(e) = sr {
            return vec![e.to_string()];
        }

        let sr = sr.unwrap_or(StringRecord::new());
        QueryResult::extract_string_record(&sr)
    }

    fn get_column_numbers(&self, cols: Vec<SelectItem>) -> Result<Vec<usize>, Box<dyn Error>> {
        if cols.len() == 1 {
            if let SelectItem::Wildcard(_) = cols[0] {
                return Ok((0..self.columns.len()).collect());
            }
        }

        cols.iter()
            .map(|col| match col {
                SelectItem::UnnamedExpr(expr) => match expr {
                    Expr::Identifier(ident) => self
                        .columns
                        .iter()
                        .position(|c| c == &ident.value)
                        .ok_or(format!("column \"{}\" is not found", &ident.value).into()),
                    _ => Err("unsupported expression in select item".into()),
                },
                _ => Err("unsupported select item".into()),
            })
            .collect()
    }

    fn extract_string_record(s: &StringRecord) -> Row {
        s.iter().map(|s| s.to_string()).collect()
    }
}

impl Iterator for QueryResult {
    type Item = Vec<String>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_row()
    }
}

