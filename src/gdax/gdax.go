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

type GdaxTicker struct {
	Price float64 `json:"price"`
	Timestamp int64 `json:"timestamp"`
}

type gdaxTickerOriginal struct {
	Price string `json:"price"`
	Time string `json:"time"`
}

func FindTickerLatest(db *sql.DB, count int32) ([]GdaxTicker, error) {
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

	var result []GdaxTicker
	for rows.Next() {
		var timestamp int64
		var price float64

		err = rows.Scan(&timestamp, &price)

		if err != nil {
			log.Printf("read gdax btcusd latest error, error=%v\n", err)
			return nil, err
		}

		result = append(result, GdaxTicker{price, timestamp})
	}

	return result, nil
}

func SaveTicker(db *sql.DB, ticker *GdaxTicker) error {
	saveSql := "INSERT INTO `gdax_btcusd_logs`(`log_time`,`log_price`) VALUES(?,?)"
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

func FetchTicker(product string) (GdaxTicker, error)  {
	url := fmt.Sprintf("https://api.gdax.com/products/%v/ticker", product)
	log.Printf("Fetch url %v\n", url)

	var result GdaxTicker

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

	original := gdaxTickerOriginal{}

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

	tm, err := time.Parse("2006-01-02T15:04:05.999999Z", original.Time)

	result = GdaxTicker{price, tm.Unix()}

	return result, nil
}

func InitDb(db *sql.DB)  {
	util.CheckAndCreateTable(db,
		"gdax_btcusd_logs",
		"CREATE TABLE `gdax_btcusd_logs` (`log_time` BIGINT PRIMARY KEY,`log_price` DECIMAL(10,2) NOT NULL,`created_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)")
}