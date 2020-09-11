package rabbitmq

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type Config struct {
	SSL               bool
	Addr              string
	User              string
	Passwd            string
	CACertPath        string
	ClientCertPath    string
	ClientPrivKeyPath string
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

func OpenConnect(cfg *Config) (*Channel, error) {

	var (
		conn      *Connection
		ch        *Channel
		err       error
	)

	tlsConfig := new(tls.Config)

	connectionURL := cfg.FormatURL()

	// Check if ssl read certs
	if cfg.SSL {
		// see example on https://github.com/streadway/amqp/blob/master/examples_test.go

		tlsConfig.RootCAs = x509.NewCertPool()

		log.Debugf("read ca certificate from %s", cfg.CACertPath)

		if ca, err := ioutil.ReadFile(cfg.CACertPath); err != nil {
			return nil, err
		} else {
			tlsConfig.RootCAs.AppendCertsFromPEM(ca)
		}

		log.Debugf("read client certificate from %s", cfg.ClientCertPath)
		log.Debugf("read client private key from %s", cfg.ClientPrivKeyPath)

		if cert, err := tls.LoadX509KeyPair(cfg.ClientCertPath, cfg.ClientPrivKeyPath); err != nil {
			return nil, err
		} else {
			tlsConfig.Certificates = append(tlsConfig.Certificates, cert)
		}

		conn, err = DialTLS(connectionURL, tlsConfig)
		if err != nil {
			return nil, err
		}
	}

	conn, err = Dial(connectionURL)
	if err != nil {
		return nil, err
	}

	ch, err = conn.Channel()
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
