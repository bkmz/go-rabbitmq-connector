package rabbitmq

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

func OpenConnect(uri string) ( *Channel, error ) {

	conn, err := Dial(uri)
	if err != nil {
		return nil, err
	} 

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return ch, nil
}

func (ch *Channel) InitStruct(prefix string) error {
	var err error

	log.Debug("rabbitmq struct initing start")

	err = ch.ExchangeDeclare(
		fmt.Sprintf("%sExchange", prefix), // name
		"fanout",                          // type
		true,                              // durable
		false,                             // auto delete
		false,                             // internal
		false,                             // no wait
		nil,                               // arguments
	)
	if err != nil {
		return err
	}

	err = ch.ExchangeDeclare(
		fmt.Sprintf("%sRetryExchange", prefix), // name
		"fanout",                               // type
		true,                                   // durable
		false,                                  // auto delete
		false,                                  // internal
		false,                                  // no wait
		nil,                                    // arguments
	)
	if err != nil {
		return err
	}

	args := amqp.Table{
		"x-dead-letter-exchange": fmt.Sprintf("%sRetryExchange", prefix),
	}
	_, err = ch.QueueDeclare(
		fmt.Sprintf("%sQueue", prefix), // name
		true,                           // durable - flush to disk
		false,                          // delete when unused
		false,                          // exclusive - only accessible by the connection that declares
		false,                          // no-wait - the queue will assume to be declared on the server
		args,                           // arguments -
	)
	if err != nil {
		return err
	}

	args = amqp.Table{
		"x-dead-letter-exchange": fmt.Sprintf("%sExchange", prefix),
		"x-message-ttl":          60000,
	}
	_, err = ch.QueueDeclare(
		fmt.Sprintf("%sRetryQueue", prefix), // name
		true,                                // durable - flush to disk
		false,                               // delete when unused
		false,                               // exclusive - only accessible by the connection that declares
		false,                               // no-wait - the queue will assume to be declared on the server
		args,                                // arguments -
	)
	if err != nil {
		return err
	}

	_, err = ch.QueueDeclare(
		fmt.Sprintf("%sArchiveQueue", prefix), // name
		true,                                  // durable - flush to disk
		false,                                 // delete when unused
		false,                                 // exclusive - only accessible by the connection that declares
		false,                                 // no-wait - the queue will assume to be declared on the server
		nil,                                   // arguments -
	)
	if err != nil {
		return err
	}

	err = ch.QueueBind(
		fmt.Sprintf("%sQueue", prefix),
		"*",
		fmt.Sprintf("%sExchange", prefix),
		false,
		nil,
	)
	if err != nil {
		return err
	}

	err = ch.QueueBind(
		fmt.Sprintf("%sArchiveQueue", prefix),
		"*",
		fmt.Sprintf("%sExchange", prefix),
		false,
		nil,
	)
	if err != nil {
		return err
	}

	err = ch.QueueBind(
		fmt.Sprintf("%sRetryQueue", prefix),
		"*",
		fmt.Sprintf("%sRetryExchange", prefix),
		false,
		nil,
	)
	if err != nil {
		return err
	}

	log.Debug("rabbitmq struct initing success")
	return nil
}
