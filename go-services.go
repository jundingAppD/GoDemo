package main

import appd "appdynamics"
import (
    "fmt"
    "github.com/Shopify/sarama"
    "io/ioutil"
    "log"
    "math/rand"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

func handler(w http.ResponseWriter, r *http.Request) {

    fmt.Println("handler: " + r.URL.Path[1:])
    hdr := r.Header.Get(appd.APPD_CORRELATION_HEADER_NAME)
    btHandle := appd.StartBT(r.URL.Path[1:], hdr)
    r2 := rand.Intn(100) + 200
    time.Sleep(time.Duration(r2) * time.Millisecond)
    fmt.Fprintf(w, "Hi there from physicsServices: " + r.URL.Path[1:] + "!")
    appd.EndBT(btHandle)
}

func gameCachingHandler(w http.ResponseWriter, r *http.Request) {

    path := r.URL.Path[1:]
    splits := strings.Split(path, "/")
    action := splits[1]

    hdr := r.Header.Get(appd.APPD_CORRELATION_HEADER_NAME)
    btHandle := appd.StartBT(r.URL.Path[1:], hdr)

    if (action == "getWorld") {
        callKafka(btHandle, "Kafka", os.Getenv("MESSAGE_TOPIC"))
    } else if (action == "updateAction") {
        callKafka(btHandle, "Kafka", os.Getenv("ANALYTICS_TOPIC"))
        callKafka(btHandle, "Kafka", os.Getenv("MESSAGE_TOPIC"))
    }

    r2 := rand.Intn(100) + 200
    time.Sleep(time.Duration(r2) * time.Millisecond)
    fmt.Fprintf(w, "Hi there from " + r.URL.Path[1:] + "!")
    appd.EndBT(btHandle)
}

func gameLogicServicesHandler(w http.ResponseWriter, r *http.Request) {

    path := r.URL.Path[1:]
    splits := strings.Split(path, "/")
    action := splits[1]

    hdr := r.Header.Get(appd.APPD_CORRELATION_HEADER_NAME)
    btHandle := appd.StartBT(r.URL.Path[1:], hdr)

    if (action == "getWorld") {
        makeWebRequest(btHandle, "physics-services", "3945", "/physicsServices/getWorld")
    }

    r2 := rand.Intn(100) + 200
    time.Sleep(time.Duration(r2) * time.Millisecond)
    fmt.Fprintf(w, "Hi there from gameLogicServices: " + r.URL.Path[1:] + "!")
    appd.EndBT(btHandle)
}

func worldGeneratorHandler(w http.ResponseWriter, r *http.Request) {

    path := r.URL.Path[1:]
    splits := strings.Split(path, "/")
    action := splits[1]

    hdr := r.Header.Get(appd.APPD_CORRELATION_HEADER_NAME)
    btHandle := appd.StartBT(path, hdr)

    if (action == "getWorld") {

        makeWebRequest(btHandle, "game-caching", "3945", "/gameCaching/getWorld")
        makeWebRequest(btHandle, "game-logic-services", "3945", "/gameLogicServices/getWorld")
    
    } else if (action == "updateAction") {
    
        makeWebRequest(btHandle, "game-caching", "3945", "/gameCaching/updateAction")
    
    } 

    appd.EndBT(btHandle)
}

func callKafka(btHandle appd.BtHandle, backendName string, topicName string) {

    exitCallHandle := appd.StartExitcall(btHandle, backendName)
    hdr := appd.GetExitcallCorrelationHeader(exitCallHandle)

    fmt.Println("Producing mesage to topic " + topicName)
    fmt.Println("kafka hdr: " + hdr)

    kafkaVersion, err := sarama.ParseKafkaVersion("2.1.0")
    if err != nil {
        panic(err)
    }

    config := sarama.NewConfig()
    config.Version = kafkaVersion
    config.Producer.Return.Successes = true
    config.Producer.RequiredAcks = sarama.NoResponse
    brokers := []string{"kafka:9092"}

    producer, err := sarama.NewSyncProducer(brokers, config )
    if err != nil {
        panic(err)
    }

    defer func() {
        if err := producer.Close(); err != nil {
            panic(err)
        }
    }()

    strTime := strconv.Itoa(int(time.Now().Unix()))
    msg := &sarama.ProducerMessage {
        Topic: topicName,
        Key:   sarama.StringEncoder(strTime),
        Value: sarama.StringEncoder("This is a message in Kafka."),
        Headers: []sarama.RecordHeader {
            sarama.RecordHeader {
                Key:   []byte(appd.APPD_CORRELATION_HEADER_NAME),
                Value: []byte(hdr),
            },
        },
    }

    partition, offset, err := producer.SendMessage(msg)
    _ = partition
    _ = offset
    if err != nil {
        fmt.Println("FAILED to send message: %s\n", err)
    } else {
        fmt.Println("Message sent successfully")
    }

    appd.EndExitcall(exitCallHandle)
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

func main() {

    f, err := os.OpenFile("/tmp/logfile.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
    if err != nil {
        log.Fatal(err)
    }
    defer f.Close()
    log.SetOutput(f)
    log.Println("check to make sure it works")

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

    backendPropertiesKafka := map[string]string{"DESTINATION": "kafka:9092", "DESTINATIONTYPE": "JMS", "VENDOR": "Kafka"}
    appd.AddBackend("Kafka", appd.APPD_BACKEND_JMS, backendPropertiesKafka, false)

    appd.AddBackend("game-caching", appd.APPD_BACKEND_HTTP, map[string]string{"HOST": "http://game-caching", "PORT": "3945"}, true)
    appd.AddBackend("physics-services", appd.APPD_BACKEND_HTTP, map[string]string{"HOST": "http://physics-services", "PORT": "3945"}, true)
    appd.AddBackend("game-logic-services", appd.APPD_BACKEND_HTTP, map[string]string{"HOST": "http://game-logic-services", "PORT": "3945"}, true)

    http.HandleFunc("/worldGenerator/", worldGeneratorHandler)
    http.HandleFunc("/gameCaching/", gameCachingHandler)
    http.HandleFunc("/gameLogicServices/", gameLogicServicesHandler)
    http.HandleFunc("/", handler)

    http.ListenAndServe(":3945", nil)
}
