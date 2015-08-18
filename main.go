package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/neilcwilkinson/messaging"

	"github.com/neilcwilkinson/authentication"
	"github.com/neilcwilkinson/mongo"
	rc "github.com/neilcwilkinson/rabbit/consumer"
	rp "github.com/neilcwilkinson/rabbit/producer"
	"golang.org/x/net/websocket"
)

type rateCompactMessage struct {
	MessageType string
	PairLookUp  int64
	Bid         float64
	Offer       float64
	Status      string
}

type rateSURMessage struct {
	MessageType       string
	PairLookUp        int64
	Product           string
	Bid               float64
	Offer             float64
	High              float64
	Low               float64
	Status            string
	Notation          string
	Decimals          int64
	ClosingBid        float64
	PairRatedelimiter string
}

func (p rateSURMessage) String() string {
	return fmt.Sprintf("%s %d %s %f %f %f %f %s %s %d %f %s\n", p.MessageType, p.PairLookUp, p.Product, p.Bid, p.Offer, p.High, p.Low, p.Status, p.Notation, p.Decimals, p.ClosingBid, p.PairRatedelimiter)
}

var (
	allClients = make(map[*websocket.Conn]ClientConnection)
)

type ClientConnection struct {
	RemoteAddr string
	Auth       bool
}

type Connection struct {
	Address string
	Token   string
}

var (
	Token = "91C5FF91DB2E7F353002B1604C5C7F66/COMPACT"
)

var (
	rateString   = make(chan string)
	RedisChannel = make(chan string)
)

func SendAuthenticationSuccess(ws *websocket.Conn, c ClientConnection) {
	body, _ := json.Marshal(c)
	message := messaging.Message{From: "Me", To: "You", Subject: "Authorization", Body: string(body)}
	if err := websocket.JSON.Send(ws, message); err != nil {
		// If there was an error communicating
		// with them, the connection is dead.
		fmt.Println("Unable to send authorization success")
	}
}

func SendAuthenticationFailure(ws *websocket.Conn, c ClientConnection) {
	body, _ := json.Marshal(c)
	message := messaging.Message{From: "Me", To: "You", Subject: "Authorization", Body: string(body)}
	if err := websocket.JSON.Send(ws, message); err != nil {
		// If there was an error communicating
		// with them, the connection is dead.
		fmt.Println("Unable to send authorization failure")
	}
}

func Receive(ws *websocket.Conn) {
	for {
		var c ClientConnection
		c = allClients[ws]

		var message messaging.Message
		if err := websocket.JSON.Receive(ws, &message); err != nil {
			fmt.Printf("Unable to receive - connection closed\r\n")
			//ws.Close()
			delete(allClients, ws)
			return
		} else {
			if strings.EqualFold(message.Subject, "Authorization") {
				valid, user := authentication.IsAuthorized(message)
				if valid {
					fmt.Println("User: ", user.Username)
					c.Auth = true
					fmt.Println("Client connected:")
					fmt.Println("Remote Address: ", c.RemoteAddr)
					fmt.Println("Authorized: ", c.Auth)

					allClients[ws] = c
					go SendAuthenticationSuccess(ws, c)
				} else {
					fmt.Println("Invalid logon attempt")
					go SendAuthenticationFailure(ws, c)
					//ws.Close()
				}
			} else {
				fmt.Println("Invalid logon attempt")
				go SendAuthenticationFailure(ws, c)
				//ws.Close()
			}
		}
	}
}

func createserver(ws *websocket.Conn) {
	var c ClientConnection
	c.RemoteAddr = ws.Request().RemoteAddr
	fmt.Printf(c.RemoteAddr + "\r\n")
	c.Auth = false

	allClients[ws] = c
	fmt.Println("Client connected:")
	fmt.Println("Remote Address: ", c.RemoteAddr)
	fmt.Println("Authorized: ", c.Auth)
	Receive(ws)
}

func parseRates() {
	go func() {
		for {
			msg := <-rateString
			ratemap := strings.SplitN(strings.TrimRight(msg, "$"), "\\", strings.LastIndex(msg, "\\"))
			if len(ratemap) > 5 {
				var r rateMessage

				r.Product = ratemap[0][len(ratemap[0])-7:]
				r.Display = ratemap[1]
				r.Bid, _ = strconv.ParseFloat(ratemap[2], 0)
				r.Offer, _ = strconv.ParseFloat(ratemap[3], 0)
				r.Status = ratemap[4]

				body, _ := json.Marshal(r)

				message := messaging.Message{From: "Me", To: "You", Subject: "Price", Body: string(body)}
				go mongo.LogMessage("currentrates", r.Product, body)
				fmt.Printf("\r%s", string(body))
				go sendmessage(message)
			}
		}
	}()
}

func main() {
	fmt.Println("We have started!!")

	fmt.Println("Attempting to connect to Rabbit..")
	go rc.Initialize()
	go rp.Initialize()

	//mongo.Initialize("localhost:27017")
	mongo.Initialize("mongodb1-betfolio-mongo-db-3kjz:27017")

	go parseRates()

	go rssConnect("demorates.efxnow.com:443", "91C5FF91DB2E7F353002B1604C5C7F66/COMPACT")
	fmt.Println("Starting httpserver")

	_, err := os.Stat("/go/bin/webcontent")
	if err != nil {
		panic("Where the f is the directory?")
	}

	http.Handle("/rates", websocket.Handler(createserver))
	http.Handle("/", http.FileServer(http.Dir("/go/bin/webcontent")))
	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		panic("ListenAndServe: " + err.Error())
	}
}

func rssConnect(rssAddr, token string) {
	//rateArray := make([]rateSURMessage, 64)

	fmt.Printf("Token received: %s %s \r\n", token, "connecting...")

	conn, err := net.Dial("tcp", rssAddr) // Establish Socket Connection to rss
	if err != nil {
		os.Exit(1)
	}

	defer conn.Close() // Post close on function exit request

	_, err = conn.Write([]byte(token)) // Send authentication token to rss
	if err != nil {
		os.Exit(1)
	}

	inputReader := bufio.NewReader(conn)

	result, err := inputReader.ReadString('\r') // Read book
	if err != nil {
		os.Exit(1)
	}

	// for idx, value := range strings.Split(result, "$") { //Loop through the book and load into an array
	// 	if idx < strings.Count(result, "$") {
	// 		buildRateTable(strings.TrimLeft(value, "S"), &rateArray[idx])
	// 	} else {
	// 		break
	// 	}
	// }

	for { //  loop forever receiving rate update messages
		result, err = inputReader.ReadString('\r')
		if err != nil {
			os.Exit(1)
		}

		rateString <- result
		//go parseRate(result)
	}
	os.Exit(0)
}

func sendmessage(message messaging.Message) {
	for conn, _ := range allClients {
		// Send them a message in a go-routine
		// so that the network operation doesn't block
		//
		go func(conn *websocket.Conn, message messaging.Message) {
			//if allClients[conn].Auth {
			if err := websocket.JSON.Send(conn, message); err != nil {
				delete(allClients, conn)
				fmt.Println("Unable to send - connection closed\r\n")
			}
			//}
		}(conn, message)
	}
}

type rateMessage struct {
	Product string
	Display string
	Bid     float64
	Offer   float64
	Status  string
}

func buildRateTable(in string, out *rateSURMessage) {
	ratemap := strings.SplitN(strings.TrimRight(in, "$"), "\\", strings.LastIndex(in, "\\"))

	out.PairLookUp, _ = strconv.ParseInt(ratemap[0], 0, 0)
	out.Product = ratemap[1]
	out.Bid, _ = strconv.ParseFloat(ratemap[2], 0)
	out.Offer, _ = strconv.ParseFloat(ratemap[3], 0)
	out.High, _ = strconv.ParseFloat(ratemap[4], 0)
	out.Low, _ = strconv.ParseFloat(ratemap[5], 0)
	out.Status = ratemap[6]
	out.Notation = ratemap[7]
	out.Decimals, _ = strconv.ParseInt(ratemap[8], 0, 0)
	out.ClosingBid, _ = strconv.ParseFloat(ratemap[9], 0)
}
