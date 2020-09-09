package rabbitmq

import (
	"bytes"
	"time"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"sync/atomic"
)


const delay = 3 // reconnect after delay seconds

// LogFunc is a function that logs the provided message with optional
// fmt.Sprintf-style arguments. By default, logs to the default log.Logger.
// var LogFuncInfo func(string, ...interface{}) = log.Printf
// var LogFuncError func(string, ...interface{}) = log.Printf

type Config struct {
	SSL    bool
	Addr   string
	User   string
	Passwd string
}

func init() {
  // Log as JSON instead of the default ASCII formatter.
  log.SetFormatter(&log.JSONFormatter{})

  // Output to stdout instead of the default stderr
  // Can be any io.Writer, see below for File example
  log.SetOutput(os.Stdout)

  // Only log the warning severity or above.
  log.SetLevel(log.WarnLevel)
}

func (cfg *Config) FormatURL() string {
	var buf bytes.Buffer

	// [ampq[s]://]
	buf.WriteString("amqp")

	if cfg.SSL {
		buf.WriteByte('s')
	}
	buf.WriteString("://")

	// user:password@
	if len(cfg.User) > 0 {
		buf.WriteString(cfg.User)
		if len(cfg.Passwd) > 0 {
			buf.WriteByte(':')
			buf.WriteString(cfg.Passwd)
		}
		buf.WriteByte('@')
	}

	// [address:port/]
	buf.WriteString(cfg.Addr)
	buf.WriteByte('/')

	return buf.String()
}

// Connection amqp.Connection wrapper
type Connection struct {
	*amqp.Connection
}

// Dial wrap amqp.Dial, dial and get a reconnect connection
func Dial(url string) (*Connection, error) {

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	log.Info("rabbitmq connection success")
	
	connection := &Connection{
		Connection: conn,
	}

	go func() {
		for {
			reason, ok := <-connection.Connection.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok {
				log.Info("rabbitmq connection closed")
				break
			}
			log.Error("rabbitmq connection closed, reason: %v", reason)

			// reconnect if not closed by developer
			for {
				// wait 1s for reconnect
				time.Sleep(delay * time.Second)

				conn, err := amqp.Dial(url)
				if err == nil {
					connection.Connection = conn
					log.Info("rabbitmq connection success")
					break
				}
				log.Error("rabbitmq reconnect failed, err: %v", err)
			}
		}
	}()

	return connection, nil
}

// Channel wrap amqp.Connection.Channel, get a auto reconnect channel
func (c *Connection) Channel() (*Channel, error) {
	ch, err := c.Connection.Channel()
	if err != nil {
		return nil, err
	}

	log.Info("rabbitmq channel opened")
	channel := &Channel{
		Channel: ch,
	}

	go func() {
		for {
			reason, ok := <-channel.Channel.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok || channel.IsClosed() {
				log.Info("channel closed")
				channel.Close() // close again, ensure closed flag set when connection closed
				break
			}
			log.Error("channel closed, reason: %v", reason)

			// reconnect if not closed by developer
			for {
				// wait 1s for connection reconnect
				time.Sleep(delay * time.Second)

				ch, err := c.Connection.Channel()
				if err == nil {
					log.Info("channel recreate success")
					channel.Channel = ch
					break
				}

				log.Error("channel recreate failed, err: %v", err)
			}
		}

	}()

	return channel, nil
}

// Channel amqp.Channel wapper
type Channel struct {
	*amqp.Channel
	closed int32
}

// IsClosed indicate closed by developer
func (ch *Channel) IsClosed() bool {
	return (atomic.LoadInt32(&ch.closed) == 1)
}

// Close ensure closed flag set
func (ch *Channel) Close() error {
	if ch.IsClosed() {
		return amqp.ErrClosed
	}

	atomic.StoreInt32(&ch.closed, 1)

	return ch.Channel.Close()
}

// Consume warp amqp.Channel.Consume, the returned delivery will end only when channel closed by developer
func (ch *Channel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	deliveries := make(chan amqp.Delivery)

	go func() {
		for {
			d, err := ch.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
			if err != nil {
				log.Error("consume failed, err: %v", err)
				time.Sleep(delay * time.Second)
				continue
			}

			for msg := range d {
				deliveries <- msg
			}

			// sleep before IsClose call. closed flag may not set before sleep.
			time.Sleep(delay * time.Second)

			if ch.IsClosed() {
				break
			}
		}
	}()

	return deliveries, nil
}

// func (ch *Channel) Publish(exchange, key string, mandatory, immediate bool, msg) error {

// 	go func() {
// 		for {
// 			err := ch.Channel.Publish(exchange, key, mandatory, immediate, msg)
// 			if err != nil {
// 				// log.Error().Msgf("publish filed, err: %v", err)
// 				time.Sleep(delay * time.Second)
// 				continue
// 			}

// 			if ch.IsClosed() {
// 				break
// 			}
// 		}
// 	}()

// 	return nil
// }
