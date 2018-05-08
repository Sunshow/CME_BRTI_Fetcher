package bitstamp

import (
	"database/sql"
	"util"
	"fmt"
	"log"
	"net/http"
	"time"
	"io/ioutil"
	"encoding/json"
	"strconv"
	"errors"
)

const ProductBtcUsd = "btcusd"

type Ticker struct {
	Timestamp int64 `json:"timestamp"`
	Price float64 `json:"price"`
	Low float64 `json:"low"`
	High float64 `json:"high"`
}

type HistoricLowest struct {
	Start int64 `json:"start"`
	End int64 `json:"end"`
	Lowest float64 `json:"lowest"`
}

type tickerOriginal struct {
	Timestamp string `json:"timestamp"`
	Last string `json:"last"`
	Low string `json:"low"`
	High string `json:"high"`
}

func FindTickerLatest(db *sql.DB, count int32) ([]Ticker, error) {
	if count < 1 || count > 100 {
		log.Printf("query count out of range: %v\n", count)
		return nil, errors.New("query count out of range")
	}
	rows, err := db.Query("SELECT `log_time`,`log_price`,`log_low_hourly`,`log_high_hourly` FROM `bitstamp_btcusd_logs` ORDER BY `log_time` DESC LIMIT ?", count)
	if err != nil {
		log.Printf("query bitstamp btcusd latest error, error=%v\n", err)
		return nil, err
	}

	defer rows.Close()

	var result []Ticker
	for rows.Next() {
		var timestamp int64
		var price float64
		var lowHourly float64
		var highHourly float64

		err = rows.Scan(&timestamp, &price, &lowHourly, &highHourly)

		if err != nil {
			log.Printf("read bitstamp btcusd latest error, error=%v\n", err)
			return nil, err
		}

		result = append(result, Ticker{timestamp, price, lowHourly, highHourly})
	}

	return result, nil
}

func FindHistoricLowest(db *sql.DB, tsStart int64, tsEnd int64) (HistoricLowest, error) {
	var result HistoricLowest

	rows, err := db.Query("SELECT `log_low_hourly` FROM `bitstamp_btcusd_logs` WHERE `log_time` BETWEEN ? AND ? ORDER BY `log_low_hourly` ASC LIMIT 1", tsStart, tsEnd)
	if err != nil {
		log.Printf("query bitstamp btcusd lowest error, error=%v\n", err)
		return result, err
	}

	defer rows.Close()

	if rows.Next() {
		var lowest float64

		err = rows.Scan(&lowest)

		if err != nil {
			log.Printf("read bitstamp btcusd lowest error, error=%v\n", err)
			return result, err
		}

		result = HistoricLowest{tsStart, tsEnd, lowest}
		return result, nil
	} else {
		return result, errors.New("lowest historic not found")
	}
}

func SaveTicker(db *sql.DB, ticker *Ticker) error {
	saveSql := "INSERT OR IGNORE INTO `bitstamp_btcusd_logs`(`log_time`,`log_price`,`log_low_hourly`,`log_high_hourly`) VALUES(?,?,?,?)"
	stmt, err := db.Prepare(saveSql)
	if err != nil {
		log.Printf("prepare stmt error: %v\n", err)
		return err
	}

	defer stmt.Close()

	res, err := stmt.Exec(ticker.Timestamp, ticker.Price, ticker.Low, ticker.High)
	if err != nil {
		log.Printf("exec save sql error: %v\n", err)
		return err
	}

	affectedRows, err := res.RowsAffected()
	if err != nil {
		log.Println(err)
		return err
	}

	if affectedRows > 0 {
		log.Printf("saved bitstamp btcusd log, %v\n", ticker)
	}

	return nil
}

func FetchTicker(product string) (Ticker, error) {
	url := fmt.Sprintf("https://www.bitstamp.net/api/v2/ticker_hour/%v/", product)
	log.Printf("Fetch url %v\n", url)

	var result Ticker

	httpClient := http.Client{
		Timeout: time.Second * 5,
	}

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		log.Println(err)
		return result, err
	}

	res, err := httpClient.Do(req)
	if err != nil {
		log.Println(err)
		return result, err
	}

	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Println(err)
		return result, err
	}

	original := tickerOriginal{}

	err = json.Unmarshal(body, &original)
	if err != nil {
		log.Println(err)
		return result, err
	}

	ts, err := strconv.ParseInt(original.Timestamp, 10, 64)
	if err != nil {
		log.Println(err)
		return result, err
	}

	price, err := strconv.ParseFloat(original.Last, 64)
	if err != nil {
		log.Println(err)
		return result, err
	}

	low, err := strconv.ParseFloat(original.Low, 64)
	if err != nil {
		log.Println(err)
		return result, err
	}

	high, err := strconv.ParseFloat(original.High, 64)
	if err != nil {
		log.Println(err)
		return result, err
	}

	result = Ticker{ts, price, low, high}

	return result, nil
}

func InitDb(db *sql.DB)  {
	util.CheckAndCreateTable(db,
		"bitstamp_btcusd_logs",
		"CREATE TABLE `bitstamp_btcusd_logs` (`log_time` BIGINT PRIMARY KEY,`log_price` DECIMAL(10,2) NOT NULL,`log_low_hourly` DECIMAL(10,2) NOT NULL,`log_high_hourly` DECIMAL(10,2) NOT NULL,`created_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)")

	util.ExecuteStmtSql(db, "CREATE INDEX IF NOT EXISTS idx_low_hourly ON `bitstamp_btcusd_logs`(`log_low_hourly`)")
	util.ExecuteStmtSql(db, "CREATE INDEX IF NOT EXISTS idx_high_hourly ON `bitstamp_btcusd_logs`(`log_high_hourly`)")
}