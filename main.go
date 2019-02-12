package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	zmq "github.com/pebbe/zmq4"
)

func main() {
	var zmqAddr, zmqTopic string
	var decodeHex, fakeSeqNum bool
	flag.StringVar(&zmqAddr, "addr", "", "bind address")
	flag.StringVar(&zmqTopic, "topic", "", "the stream to publish to")
	flag.BoolVar(&decodeHex, "decodehex", true, "decode hex strings to raw bytes")
	flag.BoolVar(&fakeSeqNum, "fakeseqnum", false, "fake a sequence number")
	flag.Parse()

	if zmqAddr == "" || zmqTopic == "" {
		fmt.Println("Usage: zmqpipe addr:port channel")
		os.Exit(1)
	}

	// Initialize ZMQ
	ctx, err := zmq.NewContext()
	if err != nil {
		fmt.Printf("Error creating zmq context: %v\n", err)
		return
	}
	defer ctx.Term()

	pub, err := ctx.NewSocket(zmq.PUB)
	if err != nil {
		fmt.Printf("Error creating socket: %v\n", err)
		return
	}
	defer pub.Close()

	fmt.Printf("Binding to %s\n", zmqAddr)
	err = pub.Bind(fmt.Sprintf("tcp://%s", zmqAddr))
	if err != nil {
		fmt.Printf("Error binding publisher: %v\n", err)
		return
	}
	time.Sleep(500 * time.Millisecond) // wait for bind

	b, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		fmt.Printf("Error reading stdin: %v\n", err)
		return
	}

	pub.Send(zmqTopic, zmq.SNDMORE)
	time.Sleep(1 * time.Millisecond)

	// if we're given a hex string, decode to raw bytes
	possiblyHex := strings.TrimSpace(string(b))
	decoded, err := hex.DecodeString(possiblyHex)

	var n int
	var flag zmq.Flag

	if fakeSeqNum {
		flag = zmq.SNDMORE
	}

	if decodeHex && err == nil {
		n, err = pub.SendBytes(decoded, flag)
	} else {
		n, err = pub.SendMessage(b, flag)
	}

	if err != nil {
		fmt.Printf("Error sending data: %v\n", err)
		return
	}
	time.Sleep(1 * time.Millisecond)

	if fakeSeqNum {
		_, err = pub.SendMessage(0)
		if err != nil {
			fmt.Printf("Error sending seqnum: %v\n", err)
			return
		}
		time.Sleep(1 * time.Millisecond)
	}

	fmt.Printf("Sent %d bytes on %s.\n", n, zmqTopic)
}
