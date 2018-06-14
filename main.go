package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"time"

	"github.com/rs/xid"
	"github.com/streadway/amqp"
)

var (
	exchangeType = flag.String("exchange-type", "topic", "Exchange type - direct|fanout|topic|x-custom")
	queue        = flag.String("queue", "test-queue", "Ephemeral AMQP queue name")
	bindingKey   = flag.String("key", "test-key", "AMQP binding key")
	consumerTag  = flag.String("consumer-tag", "simple-consumer", "AMQP consumer tag (should not be blank)")
	lifetime     = flag.Duration("lifetime", 300*time.Second, "lifetime of process before shutdown (0s=infinite)")
	config       = flag.String("config", "/etc/hapi/config.json", "API config file")
	executable   = flag.String("exec", "hapi.php", "Path to API executable")
	durable      = flag.Bool("durable", true, "Durable queue")
)

func init() {
	flag.Parse()
}

func main() {
	cfg := parseConfig()
	*consumerTag = cfg.Amqp.ConsumerTag
	c, err := NewConsumer(cfg, cfg.Amqp.Exchange, *exchangeType, *queue, *bindingKey, *consumerTag)
	if err != nil {
		log.Fatalf("%s", err)
	}

	if *lifetime > 0 {
		log.Printf("running for %s", *lifetime)
		time.Sleep(*lifetime)
	} else {
		log.Printf("running forever")
		select {}
	}

	log.Printf("shutting down")

	if err := c.Shutdown(); err != nil {
		log.Fatalf("error during shutdown: %s", err)
	}
}

type AmqpConfig struct {
	Amqp struct {
		ConsumerTag string `json:"consumer_tag"`
		Exchange    string `json:"exchange"`
		Host        string `json:"host"`
		User        string `json:"user"`
		Pass        string `json:"pass"`
		Port        string `json:"port"`
		Queue       string `json:"queue"`
		Vhost       string `json:"vhost"`
	} `json:"amqp"`
}

func parseConfig() *AmqpConfig {
	raw, err := ioutil.ReadFile(*config)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	var c AmqpConfig
	err = json.Unmarshal(raw, &c)
	return &c
}

type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	tag     string
	done    chan error
}

func NewConsumer(cfg *AmqpConfig, exchange, exchangeType, queueName, key, ctag string) (*Consumer, error) {
	c := &Consumer{
		conn:    nil,
		channel: nil,
		tag:     ctag,
		done:    make(chan error),
	}

	var err error
	amqpURI := fmt.Sprintf(
		"amqp://%s:%s@%s:%s/%s",
		cfg.Amqp.User,
		cfg.Amqp.Pass,
		cfg.Amqp.Host,
		cfg.Amqp.Port,
		cfg.Amqp.Vhost,
	)

	log.Printf(
		"dialing %q",
		fmt.Sprintf(
			"amqp://%s:<hidden>@%s:%s/%s",
			cfg.Amqp.User,
			cfg.Amqp.Host,
			cfg.Amqp.Port,
			cfg.Amqp.Vhost,
		),
	)
	c.conn, err = amqp.Dial(amqpURI)
	if err != nil {
		return nil, fmt.Errorf("Dial: %s", err)
	}

	go func() {
		log.Printf("closing: %s", <-c.conn.NotifyClose(make(chan *amqp.Error)))
	}()

	log.Printf("got Connection, getting Channel")
	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("Channel: %s", err)
	}

	log.Printf("got Channel, declaring Exchange (%q)", exchange)
	if err = c.channel.ExchangeDeclare(
		exchange,     // name of the exchange
		exchangeType, // type
		true,         // durable
		false,        // delete when complete
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return nil, fmt.Errorf("Exchange Declare: %s", err)
	}

	log.Printf("declared Exchange, declaring Queue %q", queueName)
	queue, err := c.channel.QueueDeclare(
		queueName, // name of the queue
		*durable,  // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Declare: %s", err)
	}

	log.Printf("declared Queue (%q %d messages, %d consumers), binding to Exchange (key %q)",
		queue.Name, queue.Messages, queue.Consumers, key)

	if err = c.channel.QueueBind(
		queue.Name, // name of the queue
		key,        // bindingKey
		exchange,   // sourceExchange
		false,      // noWait
		nil,        // arguments
	); err != nil {
		return nil, fmt.Errorf("Queue Bind: %s", err)
	}

	log.Printf("Queue bound to Exchange, starting Consume (consumer tag %q)", c.tag)
	deliveries, err := c.channel.Consume(
		queue.Name, // name
		c.tag,      // consumerTag,
		false,      // noAck
		false,      // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("Queue Consume: %s", err)
	}

	go handle(deliveries, c.done, c.channel, c)

	return c, nil
}

func (c *Consumer) Shutdown() error {
	// will close() the deliveries channel
	if err := c.channel.Cancel(c.tag, false); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}

	defer log.Printf("AMQP shutdown OK")

	// wait for handle() to exit
	return <-c.done
}

func handle(deliveries <-chan amqp.Delivery, done chan error, ch *amqp.Channel, c *Consumer) {
	for d := range deliveries {
		id := xid.New()
		log.Printf(
			"[%s] got %dB delivery: [%v] %s",
			id.String(),
			len(d.Body),
			d.DeliveryTag,
			d.Body,
		)
		cmd := exec.Command(*executable)
		stdin, err := cmd.StdinPipe()
		if err != nil {
			log.Printf("[%s] Got err: %v", id.String(), err)
		}
		go func() {
			defer stdin.Close()
			io.WriteString(stdin, string(d.Body))
		}()

		output, err := cmd.Output()
		if err != nil {
			log.Printf("[%s] Got err: %v", id.String(), err)
			output = []byte(fmt.Sprintf("%v", err))
		}
		log.Printf(
			"[%s] Delivering response to %s: %s",
			id.String(),
			d.ReplyTo,
			output,
		)

		err = ch.Publish(
			"",        // exchange
			d.ReplyTo, // routing key
			false,     // mandatory
			false,     // immediate
			amqp.Publishing{
				ContentType:   "application/json",
				CorrelationId: d.CorrelationId,
				Body:          output,
			})
		if err != nil {
			log.Printf(
				"[%s] Could not publish response to %s: %s",
				id.String(),
				d.ReplyTo,
				err,
			)
		}

		err = d.Ack(false)
		if err != nil {
			log.Printf("[%s] Could not ack delivery: %s", id.String(), err)
		}

	}
	log.Printf("handle: deliveries channel closed")
	if err := c.conn.Close(); err != nil {
		log.Printf("AMQP connection close error: %s", err)
	}
	done <- nil
}
