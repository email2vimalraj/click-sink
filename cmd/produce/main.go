package main

import (
    "encoding/json"
    "flag"
    "fmt"
    "log"
    "time"

    "github.com/IBM/sarama"
    "github.com/yourname/click-sink/internal/kafka"
)

func main() {
    brokers := flag.String("brokers", "localhost:9092", "Comma-separated list of Kafka brokers")
    topic := flag.String("topic", "events", "Kafka topic to produce to")
    n := flag.Int("n", 5, "Number of messages to produce")
    delay := flag.Duration("delay", 200*time.Millisecond, "Delay between messages")
    flag.Parse()

    b := split(*brokers)
    prod, err := kafka.NewSyncProducer(b, "click-sink-producer")
    if err != nil { log.Fatalf("producer: %v", err) }
    defer prod.Close()

    for i := 0; i < *n; i++ {
        msg := map[string]any{
            "user": map[string]any{"id": i + 1, "email": fmt.Sprintf("user%02d@example.com", i+1)},
            "event": map[string]any{"ts": time.Now().UTC().Format(time.RFC3339), "type": "test", "value": float64(i)},
        }
        bts, _ := json.Marshal(msg)
        m := &sarama.ProducerMessage{Topic: *topic, Value: sarama.ByteEncoder(bts)}
        _, _, err := prod.SendMessage(m)
        if err != nil { log.Fatalf("send: %v", err) }
        fmt.Printf("Produced: %s\n", string(bts))
        time.Sleep(*delay)
    }
}

func split(s string) []string {
    var out []string
    cur := ""
    for _, r := range s {
        if r == ',' { if cur != "" { out = append(out, cur); cur = "" }; continue }
        cur += string(r)
    }
    if cur != "" { out = append(out, cur) }
    return out
}
