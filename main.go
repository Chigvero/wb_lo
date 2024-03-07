package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Delivery struct {
	Delivery_id int
	Name        string `json:"name"`
	Phone       string `json:"phone"`
	Zip         string `json:"zip"`
	City        string `json:"city"`
	Address     string `json:"address"`
	Region      string `json:"region"`
	Email       string `json:"email"`
}

type Payment struct {
	ID            int
	Transaction   string `json:"transaction"`
	Request_id    string `json:"request_id"`
	Currency      string `json:"currency"`
	Provider      string `json:"provider"`
	Amount        int    `json:"amount"`
	Payment_dt    int    `json:"payment_dt"`
	Bank          string `json:"bank"`
	Delivery_cost int    `json:"delivery_cost"`
	Goods_total   int    `json:"goods_total"`
	Custom_fee    int    `json:"custom_fee"`
}
type Item struct {
	ID           int
	Chrt_id      int    `json:"chrt_id"`
	Track_number string `json:"track_number"`
	Price        int    `json:"price"`
	Rid          string `json:"rid"`
	Name         string `json:"name"`
	Sale         int    `json:"sale"`
	Size         string `json:"size"`
	Total_price  int    `json:"total_price"`
	Nm_id        int    `json:"nm_id"`
	Brand        string `json:"brand"`
	Status       int    `json:"status"`
	Order_id     int
}

type Order struct {
	Entry              string
	Track_number       string
	Locale             string
	Internal_signature string
	Customer_id        string
	Delivery_service   string
	Shardkey           string
	Sm_id              int
	Date_created       string
	Oof_shard          string
	Delivery_id        int
	Order_uid          string
	ID                 int
}
type MyTable struct {
	/*need correct*/ Order_uid string `json:"order_uid"`
	/*need correct*/ Track_number string `json:"track_number"`
	//Delivery_name string `json:"delivery_name"`
	Delivery           Delivery `json:"delivery"`
	Payment            Payment  `json:"payment"`
	Items              []Item   `json:"items"`
	Entry              string   `json:"entry"`
	Locale             string   `json:"locale"`
	Internal_signature string   `json:"internal_signature"`
	Customer_id        string   `json:"customer_id"`
	Delivery_service   string   `json:"delivery_service"`
	Shardkey           string   `json:"shardkey"`
	Sm_id              int      `json:"sm_id"`
	Date_created       string   `json:"date_created"`
	Oof_shard          string   `json:"oof_shard"`
}

var cacheOrder = make(map[int]Order, 10)
var cachePayment = make(map[int]Payment, 10)
var cacheItem = make(map[int][]Item, 10)
var cacheDelivery = make(map[int]Delivery, 10)
var DirectoryFiles = make(map[int][]byte, 10)

func PaymentSelect(conn *pgxpool.Pool, OrderID *int) {
	//payment
	rowsPayment, err := conn.Query(context.Background(), "SELECT * from payment")
	if err != nil {
		panic(err)
	}
	//var rowSPaymentlice []Payment
	for rowsPayment.Next() {
		var r Payment
		err = rowsPayment.Scan(&r.Transaction, &r.Request_id, &r.Currency, &r.Provider, &r.Amount, &r.Payment_dt, &r.Bank, &r.Delivery_cost, &r.Goods_total, &r.Custom_fee, &r.ID)
		if err != nil {
			panic(err)
		}
		cachePayment[r.ID] = r
		*OrderID = r.ID
	}
}
func DelSelect(conn *pgxpool.Pool, OrderID *int) {
	//delivery
	rowsDelivery, err := conn.Query(context.Background(), "SELECT * from delivery")
	if err != nil {
		panic(err)
	}
	for rowsDelivery.Next() {
		var D Delivery
		err = rowsDelivery.Scan(&D.Name, &D.Phone, &D.Zip, &D.City, &D.Address, &D.Region, &D.Email, &D.Delivery_id)
		if err != nil {
			panic(err)
		}
		cacheDelivery[D.Delivery_id] = D
		*OrderID = D.Delivery_id
	}
}
func OrderSelect(conn *pgxpool.Pool, OrderID *int) {
	rows, err := conn.Query(context.Background(), "SELECT * from mytable")
	if err != nil {
		panic(err)
	}
	//var order []Order
	for rows.Next() {
		var r Order
		rows.Scan(&r.Entry, &r.Track_number, &r.Locale, &r.Internal_signature, &r.Customer_id, &r.Delivery_service, &r.Shardkey, &r.Sm_id, &r.Date_created, &r.Oof_shard, &r.ID, &r.Delivery_id, &r.Order_uid)
		//order = append(order, r)
		cacheOrder[r.ID] = r
		*OrderID = r.ID
		// fmt.Println(OrderID)
	}
}
func ItemSelect(conn *pgxpool.Pool, OrderID *int) {
	for i := 1; i <= *OrderID; i++ {
		commandStr := fmt.Sprintf("SELECT * from items where ORDER_ID=%v", i)
		rowsItems, err := conn.Query(context.Background(), commandStr)
		if err != nil {
			panic(err)
		}
		rowslice := []Item{}
		for rowsItems.Next() {
			var r Item
			err = rowsItems.Scan(&r.Chrt_id, &r.Track_number, &r.Price, &r.Rid, &r.Name, &r.Sale, &r.Size, &r.Nm_id, &r.Brand, &r.Status, &r.ID, &r.Order_id)
			if err != nil {
				panic(err)
			}
			rowslice = append(rowslice, r)
			//ItemID = r.ID
		}
		cacheItem[i] = rowslice
	}
}
func main() {
	OrderID := 0
	wg := sync.WaitGroup{}
	connectstring := "postgresql://postgres:password@localhost:5432/MyDataBase"
	conn, err := pgxpool.New(context.Background(), connectstring)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("connection is successfull")
	}
	defer conn.Close()
	files, err := os.ReadDir("./files")
	if err != nil {
		panic(err)
	}
	for i, file := range files {
		if filepath.Ext(file.Name()) == ".json" {
			JsonFile, err := os.Open("./files/" + file.Name())
			if err != nil {
				panic(err)
			}
			fileval, err := io.ReadAll(JsonFile)
			DirectoryFiles[i] = fileval
			if err != nil {
				panic(err)
			}
		}
	}
	PaymentSelect(conn, &OrderID)
	DelSelect(conn, &OrderID)
	OrderSelect(conn, &OrderID)
	ItemSelect(conn, &OrderID)
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		panic(err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		panic(err)
	}
	_, _ = js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:     "Orders",
		Subjects: []string{"ORDERS.*"},
	})
	wg.Add(1)
	go func() {
		nc.Subscribe("ORDERS.new", func(m *nats.Msg) {
			mytable := MyTable{}
			OrderID++
			err = json.Unmarshal(m.Data, &mytable)
			if err != nil {
				fmt.Println("Incorrect json!")
				panic(err)
			}
			_, err = conn.Exec(context.Background(), "INSERT INTO payment  VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)", mytable.Payment.Transaction, mytable.Payment.Request_id, mytable.Payment.Currency, mytable.Payment.Provider, mytable.Payment.Amount, mytable.Payment.Payment_dt, mytable.Payment.Bank, mytable.Payment.Delivery_cost, mytable.Payment.Goods_total, mytable.Payment.Custom_fee)
			if err != nil {
				fmt.Println("!1")
				panic(err)
			}
			PaymentSelect(conn, &OrderID)
			_, err = conn.Exec(context.Background(), "INSERT INTO delivery  VALUES ($1,$2,$3,$4,$5,$6,$7)", mytable.Delivery.Name, mytable.Delivery.Phone, mytable.Delivery.Zip, mytable.Delivery.City, mytable.Delivery.Address, mytable.Delivery.Region, mytable.Delivery.Email)
			if err != nil {
				fmt.Println("!2")
				panic(err)
			}
			DelSelect(conn, &OrderID)
			_, err = conn.Exec(context.Background(), "INSERT INTO mytable (order_uid,track_number,entry,locale,internal_signature,customer_id,delivery_service,shardkey,sm_id,date_created,oof_shard)  VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)", mytable.Order_uid, mytable.Track_number, mytable.Entry, mytable.Locale, mytable.Internal_signature, mytable.Customer_id, mytable.Delivery_service, mytable.Shardkey, mytable.Sm_id, mytable.Date_created, mytable.Oof_shard)
			if err != nil {
				fmt.Println("!3")
				panic(err)
			}
			OrderSelect(conn, &OrderID)
			for _, val := range mytable.Items {
				ItemInsertString := fmt.Sprintf("INSERT INTO items (chrt_id,track_number,price,rid,name,sale,size,nm_id,brand,status,order_id ) VALUES (%v,'%v',%v,'%v','%v',%v,'%v',%v,'%v',%v,%v)", val.Chrt_id, val.Track_number, val.Price, val.Rid, val.Name, val.Sale, val.Size, val.Nm_id, val.Brand, val.Status, OrderID)
				_, err = conn.Exec(context.Background(), ItemInsertString)
				if err != nil {
					fmt.Println("!4")
					panic(err)
				}
			}
			ItemSelect(conn, &OrderID)
			fmt.Println("Received a message: ")
		})
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		for _, v := range DirectoryFiles {
			js.Publish(context.Background(), "ORDERS.new", []byte(v))
		}
		wg.Done()
	}()
	http.HandleFunc("/order", Handler)
	http.ListenAndServe(":8080", nil)
	wg.Wait()
}

func Handler(w http.ResponseWriter, r *http.Request) {
	orderIDstring := r.URL.Query().Get("id")
	orderId, err := strconv.Atoi(orderIDstring)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	item, ok := cacheItem[orderId]
	if !ok {
		http.Error(w, "order nof found", http.StatusBadRequest)
		return
	}
	ord, ok := cacheOrder[orderId]
	if !ok {
		http.Error(w, "order nof found", http.StatusBadRequest)
		return
	}
	payment, ok := cachePayment[orderId]
	if !ok {
		http.Error(w, "order nof found", http.StatusBadRequest)
		return
	}
	delivery, ok := cacheDelivery[orderId]
	if !ok {
		http.Error(w, "order nof found", http.StatusBadRequest)
		return
	}
	jsonItem, err := json.Marshal(item)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
	jsonOrd, err := json.Marshal(ord)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
	jsonPayment, err := json.Marshal(payment)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
	jsonDelivery, err := json.Marshal(delivery)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
	w.Write(jsonItem)
	w.Write(jsonDelivery)
	w.Write(jsonPayment)
	w.Write(jsonOrd)

}
