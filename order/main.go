package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/nsqio/go-nsq"
)

const (
	OrderChannel   string = "OrderChannel"
	PaymentChannel string = "PaymentChannel"
	ServicePayment string = "ServicePayment"
	ActionStart    string = "Start"
	ActionDone     string = "DoneMsg"
	ActionError    string = "ErrorMsg"
)

type Purchasing struct {
	Name   string `json:"name"`
	Amount int    `json:"amount"`
}

// Message represents the payload sent over redis pub/sub
type Message struct {
	ID      string     `json:"id"`
	Service string     `json:"service"`
	Action  string     `json:"action"`
	Message Purchasing `json:"message"`
}

var order = map[string]bool{}

type Order struct {
	p *nsq.Producer
	c *nsq.Consumer
}

func main() {
	o := &Order{}

	// Creates a producer.
	producer, err := nsq.NewProducer("127.0.0.1:4150", nsq.NewConfig())
	if err != nil {
		log.Fatal("Producer: ", err)
	}

	// Creates a consumer.
	consumer, err := nsq.NewConsumer(OrderChannel, "order", nsq.NewConfig())
	if err != nil {
		log.Fatal("Consumer: ", err)
	}

	consumer.AddHandler(o)

	err = consumer.ConnectToNSQLookupd("127.0.0.1:4161")
	if err != nil {
		log.Fatal("Connection: ", err)
	}

	o.p = producer
	o.c = consumer

	r := mux.NewRouter()
	r.HandleFunc("/create/{name}/{amount}", o.create)
	r.HandleFunc("/get-order", getOrder)
	log.Println("starting server")

	srv := &http.Server{
		Handler: r,
		Addr:    "127.0.0.1:8081",
	}

	log.Fatal(srv.ListenAndServe())
}

func (o Order) HandleMessage(message *nsq.Message) error {
	m := Message{}
	err := json.Unmarshal(message.Body, &m)
	if err != nil {
		log.Println(err)
		return err
	}

	log.Printf("recieved message with id %s ", m.ID)
	// Create Order
	if m.Action == ActionStart {
		m.Action = ActionStart
		m.Service = ServicePayment
		order[m.Message.Name] = false
		messageBody, _ := json.Marshal(m)
		err = o.p.Publish(PaymentChannel, messageBody)
		if err != nil {
			log.Printf("error publishing done-message to %s channel", PaymentChannel)
			return err
		}
		log.Printf("done message published to channel :%s", PaymentChannel)

		return nil
	}

	// Confirm Paid Order
	if m.Action == ActionDone {
		log.Println("ACTION DONE")
		m.Action = ActionDone
		order[m.Message.Name] = true

		return nil
	}

	// Rollback
	if m.Action == ActionError {
		delete(order, m.Message.Name)
		log.Printf("rolling back transaction with ID :%s", m.ID)

		return nil
	}

	return nil
}

func (o Order) create(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	name := vars["name"]
	amount := vars["amount"]
	v, _ := strconv.Atoi(amount)

	order[name] = false

	m := Message{
		ID:      "1",
		Service: ServicePayment,
		Action:  ActionStart,
		Message: Purchasing{
			Name:   name,
			Amount: v,
		},
	}

	messageBody, _ := json.Marshal(m)
	err := o.p.Publish(PaymentChannel, messageBody)
	if err != nil {
		log.Printf("error publishing done-message to %s channel", PaymentChannel)

		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	log.Printf("done message published to channel :%s", PaymentChannel)

	w.WriteHeader(http.StatusOK)

	w.Write([]byte(fmt.Sprintf(`{"id": %v}`, len(order)-1)))
}

func getOrder(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)

	b, _ := json.Marshal(order)

	w.Write(b)
	// fmt.Println(balance)
}
