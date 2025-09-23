package kafka

import "errors"

var (
	ErrKafkaWriterClose = errors.New("error closing kafka writer")
	ErrKafkaReaderClose = errors.New("error closing kafka reader")
)
