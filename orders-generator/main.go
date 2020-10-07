package main

import (
	"database/sql"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/stdlib"
)

const createQ = `CREATE TABLE inventory.orders_info (
	orderid SERIAL NOT NULL PRIMARY KEY,
	custid INTEGER NOT NULL,
	amount INTEGER NOT NULL,
	city VARCHAR(255) NOT NULL
  );`

const dropQ = `drop TABLE inventory.orders_info`

var db *sql.DB
var cities []string

func main() {
	// wait for container to start
	time.Sleep(5 * time.Second)
	var err error
	db, err = sql.Open("pgx", "host=postgres port=5432 user=postgres password=postgres dbname=postgres")

	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	create()
	defer drop()

	cities = []string{"New Delhi", "Seattle", "New York", "Austin", "Chicago", "Cleveland"}
	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-exit:
			log.Println("application stopped")
			return
		default:
			insert()
			time.Sleep(3 * time.Second)
		}
	}
}

func create() {
	_, err := db.Exec(createQ)
	if err != nil {
		log.Fatal("failed to create table", err)
		return
	}
	log.Println("table created")
}

func drop() {
	_, err := db.Exec(dropQ)
	if err != nil {
		log.Fatal("failed to drop table ", err)
		return
	}
	log.Println("table dropped")
}

func insert() {
	q := "insert into inventory.orders_info (custid, amount, city) values ($1,$2,$3)"
	custid := rand.Intn(1000) + 1
	amount := rand.Intn(100) + 100
	city := cities[rand.Intn(len(cities))]

	_, err := db.Exec(q, custid, amount, city)
	if err != nil {
		log.Println("failed to insert order", err)
		return
	}
}
