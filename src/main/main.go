package main

import (
	"fmt"
	"net/http"
	"time"
	"log"
	"io/ioutil"
	"encoding/json"
	"sync"
	"path/filepath"
	"os"
	_ "github.com/mattn/go-sqlite3"
	"database/sql"
)

type BRTI struct {
	Value float64
	Date string
}

func main()  {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("running path: %v", dir)

	dbPath := fmt.Sprintf("%v/brti.db", dir)
	log.Printf("running db: %v", dbPath)

	initDb(dbPath)

	maxConcurrent := 3

	wg := sync.WaitGroup{}
	wg.Add(maxConcurrent)

	for i := 0; i < maxConcurrent; i++ {
		go func() {
			defer wg.Done()

			brti, err:= fetch()
			if err != nil {
				log.Println(err)
				return
			}

			log.Printf("fetched brti price=%v, date=%v\n", brti.Value, brti.Date)

			brtiTime, err := time.Parse("2006-01-02 15:04:05", brti.Date)
			if err != nil {
				log.Printf("parse brti date error: %v\n", brti.Date)
				return
			}

			ts := brtiTime.Unix()

			log.Printf("fetched brti timestamp: %v\n", ts)

			db, err := sql.Open("sqlite3", dbPath)
			if err != nil {
				log.Printf("open db error: %v\n", err)
				return
			}

			defer db.Close()

			saveSql := "INSERT OR IGNORE INTO `brti_logs` VALUES(?,?,NULL)"
			stmt, err := db.Prepare(saveSql)
			if err != nil {
				log.Printf("prepare stmt error: %v\n", err)
				return
			}

			defer stmt.Close()

			res, err := stmt.Exec(ts, brti.Value)
			if err != nil {
				log.Printf("exec save sql error: %v\n", err)
				return
			}

			affect, err := res.RowsAffected()
			if err != nil {
				log.Println(err)
				return
			}

			if affect > 0 {
				log.Printf("saved brti log, timestamp=%v, price=%v\n", ts, brti.Value)
			}
		}()
	}

	wg.Wait()
}

func fetch() (BRTI, error) {
	url := fmt.Sprintf("https://www.cmegroup.com/CmeWS/mvc/Bitcoin/BRTI?_=%v", time.Now().Unix())
	log.Printf("Fetch url %v\n", url)

	var brti BRTI

	httpClient := http.Client{
		Timeout: time.Second * 5,
	}

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		log.Println(err)
		return brti, err
	}

	res, getErr := httpClient.Do(req)
	if getErr != nil {
		log.Println(getErr)
		return brti, getErr
	}

	defer res.Body.Close()

	body, readErr := ioutil.ReadAll(res.Body)
	if readErr != nil {
		log.Println(readErr)
		return brti, readErr
	}

	brti = BRTI{}

	jsonErr := json.Unmarshal(body, &brti)
	if jsonErr != nil {
		log.Println(jsonErr)
		return brti, jsonErr
	}

	return brti, nil
}

func initDb(dbPath string) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name='brti_logs'")
	if err != nil {
		log.Fatal(err)
	}

	if !rows.Next() {
		log.Println("init table brti_logs")
		createTableSql := "CREATE TABLE `brti_logs` (`log_time` BIGINT PRIMARY KEY,`log_price` DECIMAL(10,2) NOT NULL,`created_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)"

		stmt, err := db.Prepare(createTableSql)
		if err != nil {
			log.Fatal(err)
		}

		defer stmt.Close()

		_, err = stmt.Exec()
		if err != nil {
			log.Fatal(err)
		}

		log.Println("init table success")
	}
}