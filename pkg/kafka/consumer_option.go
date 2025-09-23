package kafka

// ConsumerOption is how options for the Consumer are set up.
type ConsumerOption func(*Consumer)

// AutoCommitOffset sets up a Consumer with auto commiting offset after reading messages.
func AutoCommitOffset() ConsumerOption {
	return func(c *Consumer) {
		c.autoCommitOffset = true
	}
}

// ManuallyCommitOffset sets up a Consumer with manually commiting offset after reading messages.
// This configuration requires calling Confirm() after successfully processing the message for commit the offset.
func ManuallyCommitOffset() ConsumerOption {
	return func(c *Consumer) {
		c.autoCommitOffset = false
	}
}
