package main

import appd "appdynamics"
import (
	"fmt"
	"github.com/Shopify/sarama"
	"io/ioutil"
    "math/rand"
    "net/http"
	"os"
	"os/signal"
    "strconv"
	"time"
)

func main() {

    portNumber, _ := strconv.ParseUint(os.Getenv("CONTROLLER_PORT"), 10, 16)

    cfg := appd.Config{}
    cfg.AppName = os.Getenv("APPLICATION_NAME")
    cfg.TierName = os.Getenv("TIER_NAME")
    cfg.NodeName = os.Getenv("TIER_NAME") + "-0"
    cfg.Controller.Host = os.Getenv("CONTROLLER_HOST")
    cfg.Controller.Port = uint16(portNumber)
    cfg.Controller.UseSSL = false
    cfg.Controller.Account = os.Getenv("ACCOUNT_NAME")
    cfg.Controller.AccessKey = os.Getenv("ACCOUNT_ACCESS_KEY")
    cfg.InitTimeoutMs = 1000  // Wait up to 1s for initialization to finish

    appd.InitSDK(&cfg)

    appd.AddBackend("cache-services", appd.APPD_BACKEND_HTTP, map[string]string{"HOST": "http://cache-services", "PORT": "3945"}, true)

	kafkaVersion, err := sarama.ParseKafkaVersion("2.1.0")
	if err != nil {
		panic(err)
	}

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = kafkaVersion

	brokers := []string{"kafka:9092"}

	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	consumer, err := master.ConsumePartition(os.Getenv("TOPIC_NAME"), 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Count how many message processed
	msgCount := 0

	// Get signnal for finish
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++

				fmt.Println("Received message: ", string(msg.Key), string(msg.Value))

				correlationHeader := ""

				for _, h := range msg.Headers {
					strKey := fmt.Sprintf("%s", h.Key)
					strValue := fmt.Sprintf("%s", h.Value)

					if (strKey == appd.APPD_CORRELATION_HEADER_NAME) {
						correlationHeader = strValue
						fmt.Println("Key: " + strKey + ", Value: " + strValue)
					}
				}

				if (len(correlationHeader) > 0) {

					btHandle := appd.StartBT("ReceiveKafkaMessage", correlationHeader)
					fmt.Println("correlationHeader: " + correlationHeader)

             				r2 := rand.Intn(300) + 200
    					time.Sleep(time.Duration(r2) * time.Millisecond)

					if (os.Getenv("CALL_CACHE_SERVICES") == "1") {
						makeWebRequest(btHandle, "cache-services", "3945", "/cacheServices/getWorld")
					}

					//r2 := rand.Intn(100) + 200
					//time.Sleep(time.Duration(r2) * time.Millisecond)
					appd.EndBT(btHandle)
				}

			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCount, "messages")
}

func makeWebRequest(btHandle appd.BtHandle, hostName string, port string, path string) {

    fmt.Printf("Calling %s\n", path)

    exitCallHandle := appd.StartExitcall(btHandle, hostName)
    hdr := appd.GetExitcallCorrelationHeader(exitCallHandle)

    r2 := rand.Intn(100) + 200
    time.Sleep(time.Duration(r2) * time.Millisecond)

    url := "http://" + hostName + ":" + port + path;
    req, err := http.NewRequest("GET", url, nil)
    if err != nil {
        panic(err)
    }

    req.Header.Set(appd.APPD_CORRELATION_HEADER_NAME, hdr)
    client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        panic(err)
    }

    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        panic(err)
    }

    fmt.Println(string(body))
    appd.EndExitcall(exitCallHandle)
}
