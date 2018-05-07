package util

import (
	"database/sql"
	"fmt"
	"log"
)

func OpenDB(dbPath string) (*sql.DB, error) {
	return sql.Open("sqlite3", dbPath)
}

func CheckAndCreateTable(db *sql.DB, tableName string, initSql string)  {
	rows, err := db.Query(fmt.Sprintf("SELECT name FROM sqlite_master WHERE type='table' AND name='%v'", tableName))
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	if !rows.Next() {
		log.Printf("init table %v\n", tableName)

		err = ExecuteStmtSql(db, initSql)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("init table %v success\n", tableName)
	}
}

func ExecuteStmtSql(db *sql.DB, sql string) error {
	stmt, err := db.Prepare(sql)
	if err != nil {
		return err
	}

	defer stmt.Close()

	_, err = stmt.Exec()
	if err != nil {
		return err
	}

	return nil
}
