default:
	./target/debug/sql_from_csv "SELECT Index, \"User Id\", \"First Name\" FROM people.csv  LIMIT 11"


explain:
	./target/debug/sql_from_csv "EXPLAIN people.csv"

build:
	cargo build -o q

rm: 
	rm ./target/debug/sql_from_csv




