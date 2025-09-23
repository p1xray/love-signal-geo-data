package kafka

type Message struct {
	Key       string
	Topic     string
	Partition int
	Offset    int64
	Data      []byte
}
