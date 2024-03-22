package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/memphisdev/memphis-functions.go/memphis"
	mem "github.com/memphisdev/memphis.go"
)

func produceMessage(inputs map[string]string, headers map[string]string, message []byte) {
	accountId, _ := strconv.Atoi(inputs["accountId"])
	conn, err := mem.Connect(inputs["host"], inputs["username"], mem.Password(inputs["password"]), mem.AccountId(accountId))
	if err != nil {
		os.Exit(1)
	}
	defer conn.Close()

	p, err := conn.CreateProducer(headers["client"], "router-memphis")
	if err != nil {
		fmt.Printf("Producer failed: %v", err)
		os.Exit(1)
	}

	hdrs := mem.Headers{}
	hdrs.New()
	err = hdrs.Add("key", "value")
	if err != nil {
		fmt.Printf("Header failed: %v", err)
		os.Exit(1)
	}

	err = p.Produce(message, mem.MsgHeaders(hdrs))
	if err != nil {
		fmt.Printf("Produce failed: %v", err)
	}
}

// https://github.com/memphisdev/memphis.go#creating-a-memphis-function
func EventHandler(message []byte, headers map[string]string, inputs map[string]string) ([]byte, map[string]string, error) {
	// Here is a short example of converting the message payload to bytes and back

	// var event map[string]interface{}
	// json.Unmarshal(message, &event)
	// event[inputs["field_to_ingest"]] = "Hello from Memphis!"

	// // Return the payload back as []bytes
	// eventBytes, _ := json.Marshal(event)

	produceMessage(inputs, headers, message)
	return message, headers, nil
}

func main() {
	memphis.CreateFunction(EventHandler)
}
