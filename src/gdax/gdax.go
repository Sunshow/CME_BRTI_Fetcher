package gdax

import (
	"fmt"
	"log"
	"net/http"
	"time"
	"io/ioutil"
	"encoding/json"
	"strconv"
	"database/sql"
	"util"
	"errors"
)

const ProductBtcUsd = "BTC-USD"

const timeLayoutOriginal = "2006-01-02T15:04:05.999999Z"

type Ticker struct {
	Price float64 `json:"price"`
	Timestamp int64 `json:"timestamp"`
}

type Historic struct {
	Time int64 `json:"time"`
	Low float64 `json:"low"`
	High float64 `json:"high"`
	Open float64 `json:"open"`
	Close float64 `json:"close"`
}

type tickerOriginal struct {
	Price string `json:"price"`
	Time string `json:"time"`
}

func FindTickerLatest(db *sql.DB, count int32) ([]Ticker, error) {
	if count < 1 || count > 100 {
		log.Printf("query count out of range: %v\n", count)
		return nil, errors.New("query count out of range")
	}
	rows, err := db.Query("SELECT `log_time`,`log_price` FROM `gdax_btcusd_logs` ORDER BY `log_time` DESC LIMIT ?", count)
	if err != nil {
		log.Printf("query gdax btcusd latest error, error=%v\n", err)
		return nil, err
	}

	defer rows.Close()

	var result []Ticker
	for rows.Next() {
		var timestamp int64
		var price float64

		err = rows.Scan(&timestamp, &price)

		if err != nil {
			log.Printf("read gdax btcusd latest error, error=%v\n", err)
			return nil, err
		}

		result = append(result, Ticker{price, timestamp})
	}

	return result, nil
}

func FindHistoricLowest(db *sql.DB, tsStart int64, tsEnd int64) (Historic, error) {
	var result Historic

	rows, err := db.Query("SELECT `log_time`,`log_low`,`log_high`,`log_open`,`log_close` FROM `gdax_btcusd_historic` WHERE `log_time` BETWEEN ? AND ? ORDER BY `log_low` ASC LIMIT 1", tsStart, tsEnd)
	if err != nil {
		log.Printf("query gdax btcusd lowest error, error=%v\n", err)
		return result, err
	}

	defer rows.Close()

	if rows.Next() {
		var timestamp int64
		var low float64
		var high float64
		var openPrice float64
		var closePrice float64

		err = rows.Scan(&timestamp, &low, &high, &openPrice, &closePrice)

		if err != nil {
			log.Printf("read gdax btcusd lowest error, error=%v\n", err)
			return result, err
		}

		result = Historic{timestamp, low, high, openPrice, closePrice}
		return result, nil
	} else {
		return result, errors.New("lowest historic not found")
	}
}

func SaveTicker(db *sql.DB, ticker *Ticker) error {
	if ticker.Price <= 0 {
		log.Printf("ignore invalid data: %v\n", ticker)
		return errors.New("invalid price")
	}

	saveSql := "INSERT OR IGNORE INTO `gdax_btcusd_logs`(`log_time`,`log_price`) VALUES(?,?)"
	stmt, err := db.Prepare(saveSql)
	if err != nil {
		log.Printf("prepare stmt error: %v\n", err)
		return err
	}

	defer stmt.Close()

	res, err := stmt.Exec(ticker.Timestamp, ticker.Price)
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
		log.Printf("saved gdax btcusd log, %v\n", ticker)
	}

	return nil
}

func FetchTicker(product string) (Ticker, error)  {
	url := fmt.Sprintf("https://api.gdax.com/products/%v/ticker", product)
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

	price, err := strconv.ParseFloat(original.Price, 64)
	if err != nil {
		log.Println(err)
		return result, err
	}

	tm, err := time.Parse(timeLayoutOriginal, original.Time)

	result = Ticker{price, tm.Unix()}

	return result, nil
}

func SaveHistoric(db *sql.DB, historics []Historic) error {
	saveSql := "INSERT OR IGNORE INTO `gdax_btcusd_historic`(`log_time`,`log_low`,`log_high`,`log_open`,`log_close`) VALUES(?,?,?,?,?)"
	stmt, err := db.Prepare(saveSql)
	if err != nil {
		log.Printf("prepare stmt error: %v\n", err)
		return err
	}

	defer stmt.Close()

	for _, v := range historics {
		if v.Open <= 0 {
			log.Printf("ignore invalid data: %v\n", v)
			continue
		}
		_, err := stmt.Exec(v.Time, v.Low, v.High, v.Open, v.Close)
		if err != nil {
			log.Printf("exec save sql error: %v\n", err)
			return err
		}
	}

	return nil
}

func FetchHistoric(product string, tsStart int64, tsEnd int64) ([]Historic, error) {
	tmStart := time.Unix(tsStart, 0).UTC()
	tmEnd := time.Unix(tsEnd, 0).UTC()

	url := fmt.Sprintf("https://api.gdax.com/products/%v/candles?start=%v&end=%v", product, tmStart.Format(timeLayoutOriginal), tmEnd.Format(timeLayoutOriginal))
	log.Printf("Fetch url %v\n", url)

	var result []Historic

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

	var original [][]float64

	err = json.Unmarshal(body, &original)
	if err != nil {
		log.Println(err)
		return result, err
	}

	for _, v := range original {
		result = append(result, Historic{int64(v[0]), v[1], v[2], v[3], v[4]})
	}

	return result, nil
}

func InitDb(db *sql.DB)  {
	util.CheckAndCreateTable(db,
		"gdax_btcusd_logs",
		"CREATE TABLE `gdax_btcusd_logs` (`log_time` BIGINT PRIMARY KEY,`log_price` DECIMAL(10,2) NOT NULL,`created_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)")

	util.CheckAndCreateTable(db,
		"gdax_btcusd_historic",
		"CREATE TABLE `gdax_btcusd_historic` (`log_time` BIGINT PRIMARY KEY,`log_low` DECIMAL(10,2) NOT NULL,`log_high` DECIMAL(10,2) NOT NULL,`log_open` DECIMAL(10,2) NOT NULL,`log_close` DECIMAL(10,2) NOT NULL,`created_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)")

	util.ExecuteStmtSql(db, "CREATE INDEX IF NOT EXISTS idx_high ON `gdax_btcusd_historic`(`log_high`)")

	util.ExecuteStmtSql(db, "CREATE INDEX IF NOT EXISTS idx_low ON `gdax_btcusd_historic`(`log_low`)")
}