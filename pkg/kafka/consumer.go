package kafka

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
)

// Consumer provides access to the kafka reader.
type Consumer struct {
	reader           *kafka.Reader
	output           chan Message
	notify           chan error
	autoCommitOffset bool
}

// NewConsumerGroup returns new instance of Consumer which configured as consumer group.
func NewConsumerGroup(address []string, groupID, topic string, setters ...ConsumerOption) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: address,
		GroupID: groupID,
		Topic:   topic,
	})

	consumer := &Consumer{
		reader: reader,
		notify: make(chan error),
		output: make(chan Message),
	}

	for _, setter := range setters {
		setter(consumer)
	}

	return consumer
}

// Consume reads data from kafka.
func (c *Consumer) Consume(ctx context.Context) {
	defer close(c.notify)
	defer close(c.output)

	for {
		var msg kafka.Message
		var err error
		if c.autoCommitOffset {
			msg, err = c.reader.ReadMessage(ctx)
		} else {
			msg, err = c.reader.FetchMessage(ctx)
		}

		if err != nil {
			c.notify <- err
		}

		receivedMessage := Message{
			Key:       string(msg.Key),
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
			Data:      msg.Value,
		}
		c.output <- receivedMessage
	}
}

// Confirm confirms that the message was processed successfully.
func (c *Consumer) Confirm(ctx context.Context, messages ...Message) {
	if c.autoCommitOffset {
		return
	}

	kafkaMessagesToCommit := make([]kafka.Message, len(messages))
	for i, msg := range messages {
		kafkaMessagesToCommit[i] = kafka.Message{
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.Offset,
		}
	}

	if err := c.reader.CommitMessages(ctx, kafkaMessagesToCommit...); err != nil {
		c.notify <- err
	}
}

// Notify - notifies about kafka reader errors.
func (c *Consumer) Notify() <-chan error {
	return c.notify
}

// Output - provides access to the received data from kafka.
func (c *Consumer) Output() <-chan Message {
	return c.output
}

// Close - closes connection to kafka reader.
func (c *Consumer) Close() error {
	if err := c.reader.Close(); err != nil {
		return fmt.Errorf("%w: %w", ErrKafkaReaderClose, err)
	}

	return nil
}
