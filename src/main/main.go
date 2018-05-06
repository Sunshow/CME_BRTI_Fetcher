package main

import (
	"fmt"
	"net/http"
	"time"
	"log"
	"io/ioutil"
	"encoding/json"
	"path/filepath"
	"os"
	_ "github.com/mattn/go-sqlite3"
	"database/sql"
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
	"strconv"
)

type FetcherConfig struct {
	FetchBRTI bool
	FetchBitstamp bool
}

type BRTI struct {
	Value float64 `json:"value"`
	Date string `json:"date"`
}

type BRTIRESP struct {
	Timestamp int64 `json:"timestamp"`
	Price float64 `json:"price"`
}

type BitstampBtcUsd struct {
	Last string `json:"last"`
	Timestamp string `json:"timestamp"`
	High string `json:"high"`
	Low string `json:"low"`
}

type BitstampBtcUsdResp struct {
	Value float64 `json:"value"`
	Date string `json:"date"`
	High float64 `json:"high"`
	Low float64 `json:"low"`
}

func initConfig(configPath string) (FetcherConfig, error) {
	viper.SetDefault("FetchBRTI", "false")
	viper.SetDefault("FetchBitstamp", "true")

	viper.SetConfigName("config")
	viper.AddConfigPath(configPath)

	var config FetcherConfig

	err := viper.ReadInConfig()
	if err != nil {
		log.Println(err)
		switch err.(type) {
		case viper.ConfigFileNotFoundError:
			// did nothing
			break
		default:
			log.Println(err)
			return config, err
		}
	}

	config.FetchBRTI = viper.GetBool("FetchBRTI")
	config.FetchBitstamp = viper.GetBool("FetchBitstamp")

	return config, nil
}

func main()  {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("running path: %v", dir)

	log.Printf("running config: %v", dir)
	config, err := initConfig(dir)
	if err != nil {
		log.Fatal(err)
	}

	dbPath := fmt.Sprintf("%v/brti.db", dir)
	log.Printf("running db: %v", dbPath)

	initDb(dbPath)

	if config.FetchBRTI {
		go func() {
			for {
				fetch(dbPath)
				time.Sleep(time.Millisecond * 500)
			}
		}()
	}

	if config.FetchBitstamp {
		go func() {
			for {
				fetchAndSaveBitstampBtcUsd(dbPath)
				time.Sleep(time.Second * 30)
			}
		}()
	}

	r := gin.Default()

	r.GET("/brti/timestamp/:timestamp", func(c *gin.Context) {
		ts := c.Param("timestamp")

		db, err := sql.Open("sqlite3", dbPath)
		if err != nil {
			log.Printf("open db error: %v\n", err)
			return
		}

		defer db.Close()

		rows, err := db.Query("SELECT log_time,log_price FROM `brti_logs` WHERE `log_time`=?", ts)
		if err != nil {
			log.Printf("query brit by timestamp error, timestamp=%v, error=%v\n", ts, err)
			c.JSON(http.StatusBadRequest, gin.H{
				"message": "error",
			})
			return
		}

		defer rows.Close()

		if !rows.Next() {
			c.JSON(http.StatusNotFound, gin.H{
				"message": "not found",
			})
			return
		} else {
			var timestamp int64
			var price float64

			err = rows.Scan(&timestamp, &price)

			if err != nil {
				log.Printf("read brit by timestamp error, timestamp=%v, error=%v\n", ts, err)
				c.JSON(http.StatusInternalServerError, gin.H{
					"message": "internal server error",
				})
				return
			}

			c.JSON(http.StatusOK, gin.H{
				"timestamp": timestamp,
				"price": price,
			})
		}
	})

	r.GET("/brti/latest", func(c *gin.Context) {
		db, err := sql.Open("sqlite3", dbPath)
		if err != nil {
			log.Printf("open db error: %v\n", err)
			return
		}

		defer db.Close()

		rows, err := db.Query("SELECT log_time,log_price FROM `brti_logs` ORDER BY `log_time` DESC LIMIT 10")
		if err != nil {
			log.Printf("query brit latest error, error=%v\n", err)
			c.JSON(http.StatusBadRequest, gin.H{
				"message": "error",
			})
			return
		}

		defer rows.Close()

		var resp []BRTIRESP
		for rows.Next() {
			var timestamp int64
			var price float64

			err = rows.Scan(&timestamp, &price)

			if err != nil {
				log.Printf("read brit latest error, error=%v\n", err)
				c.JSON(http.StatusInternalServerError, gin.H{
					"message": "internal server error",
				})
				return
			}

			resp = append(resp, BRTIRESP{timestamp, price})
		}
		c.JSON(http.StatusOK, resp)
	})

	r.Run() // listen and serve on 0.0.0.0:8080
}

func fetch(dbPath string)  {
	maxConcurrent := 3

	for i := 0; i < maxConcurrent; i++ {
		go func() {
			brti, err:= fetchOnce()
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

			saveSql := "INSERT OR IGNORE INTO `brti_logs`(`log_time`,`log_price`) VALUES(?,?)"
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

			affectedRows, err := res.RowsAffected()
			if err != nil {
				log.Println(err)
				return
			}

			if affectedRows > 0 {
				log.Printf("saved brti log, timestamp=%v, price=%v\n", ts, brti.Value)
			}
		}()
	}
}

func fetchOnce() (BRTI, error) {
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

func fetchAndSaveBitstampBtcUsd(dbPath string)  {
	bitstamp, err:= fetchBitstampBtcUsdOnce()
	if err != nil {
		log.Println(err)
		return
	}

	log.Printf("fetched bitstamp price=%v, timestamp=%v\n", bitstamp.Last, bitstamp.Timestamp)

	ts, err := strconv.ParseInt(bitstamp.Timestamp, 10, 64)
	if err != nil {
		log.Println(err)
		return
	}

	bitstampTime := time.Unix(ts, 0)
	log.Printf("fetched bitstamp date=%v", bitstampTime.UTC().Format("2006-01-02 15:04:05"))

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Printf("open db error: %v\n", err)
		return
	}

	defer db.Close()

	saveSql := "INSERT INTO `bitstamp_btcusd_logs`(`log_time`,`log_price`,`log_low_hourly`,`log_high_hourly`) VALUES(?,?,?,?)"
	stmt, err := db.Prepare(saveSql)
	if err != nil {
		log.Printf("prepare stmt error: %v\n", err)
		return
	}

	defer stmt.Close()

	res, err := stmt.Exec(ts, bitstamp.Last, bitstamp.Low, bitstamp.High)
	if err != nil {
		log.Printf("exec save sql error: %v\n", err)
		return
	}

	affectedRows, err := res.RowsAffected()
	if err != nil {
		log.Println(err)
		return
	}

	if affectedRows > 0 {
		log.Printf("saved bitstamp btcusd log, timestamp=%v, price=%v\n", ts, bitstamp.Last)
	}
}

func fetchBitstampBtcUsdOnce() (BitstampBtcUsd, error) {
	url := fmt.Sprintf("https://www.bitstamp.net/api/v2/ticker_hour/btcusd/")
	log.Printf("Fetch url %v\n", url)

	var bitstamp BitstampBtcUsd

	httpClient := http.Client{
		Timeout: time.Second * 5,
	}

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		log.Println(err)
		return bitstamp, err
	}

	res, getErr := httpClient.Do(req)
	if getErr != nil {
		log.Println(getErr)
		return bitstamp, getErr
	}

	defer res.Body.Close()

	body, readErr := ioutil.ReadAll(res.Body)
	if readErr != nil {
		log.Println(readErr)
		return bitstamp, readErr
	}

	bitstamp = BitstampBtcUsd{}

	jsonErr := json.Unmarshal(body, &bitstamp)
	if jsonErr != nil {
		log.Println(jsonErr)
		return bitstamp, jsonErr
	}

	return bitstamp, nil
}

func initDb(dbPath string) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	checkAndCreateTable(db,
		"brti_logs",
		"CREATE TABLE `brti_logs` (`log_time` BIGINT PRIMARY KEY,`log_price` DECIMAL(10,2) NOT NULL,`created_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)")

	checkAndCreateTable(db,
		"bitstamp_btcusd_logs",
		"CREATE TABLE `bitstamp_btcusd_logs` (`log_time` BIGINT PRIMARY KEY,`log_price` DECIMAL(10,2) NOT NULL,`log_low_hourly` DECIMAL(10,2) NOT NULL,`log_high_hourly` DECIMAL(10,2) NOT NULL,`created_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)")

	executeStmtSql(db, "CREATE INDEX idx_low_hourly ON `bitstamp_btcusd_logs`(`log_low_hourly`)")
	executeStmtSql(db, "CREATE INDEX idx_high_hourly ON `bitstamp_btcusd_logs`(`log_high_hourly`)")
}

func checkAndCreateTable(db *sql.DB, tableName string, initSql string)  {
	rows, err := db.Query(fmt.Sprintf("SELECT name FROM sqlite_master WHERE type='table' AND name='%v'", tableName))
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	if !rows.Next() {
		log.Printf("init table %v\n", tableName)

		err = executeStmtSql(db, initSql)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("init table %v success\n", tableName)
	}
}

func executeStmtSql(db *sql.DB, sql string) error {
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