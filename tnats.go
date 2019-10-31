package tnats

import (
	"errors"
	"time"

	"github.com/nanux-io/nanux/handler"

	"github.com/nats-io/go-nats"

	log "github.com/sirupsen/logrus"
)

// Nats define a nats instance
type Nats struct {
	conn *nats.Conn
	// The key corresponds to the subject associate to the action
	actions      map[string]handler.ListenerAction
	errorHandler handler.ManageError
	closeChan    chan bool
	isClosed     bool
}

/*----------------------------------------------------------------------------------*\
// Options for actions handled by nats
\*----------------------------------------------------------------------------------*/

// NatsOptIsQueued is the name for the option telling to nat that the action must be queued.
// It means that only one subscriber will respond to the request
const NatsOptIsQueued handler.OptName = "NATS_IS_QUEUED"

// Listen initialize the connection to nats server and subscribe to
// the actions specified using Addhandler.
func (nt *Nats) Listen() error {

	if nt.isClosed {
		errMsg := "The nats connection has been closed and can not be open again"
		log.Errorln(errMsg)

		return errors.New(errMsg)
	}

	if nt.conn == nil || nt.conn.Status() != nats.CONNECTED {
		errMsg := "The nats connection is either nil or do not have status connected"
		log.Errorln(errMsg)

		return errors.New(errMsg)
	}

	if err := nt.subscribeAll(); err != nil {
		nt.conn.Close()
		return err
	}

	nt.conn.Flush()

	if err := nt.conn.LastError(); err != nil {
		log.Fatal(err)
	}

	log.Infoln("Service listening with nats")

	// Wait to receive data from the close channel to close the connection
	<-nt.closeChan

	return nil
}

// HandleAction add the action and associate it to the subject. This action will be
// used when listen is called.
func (nt *Nats) HandleAction(subject string, action handler.ListenerAction) error {
	// Check if an action is already associated to the subject
	if _, ok := nt.actions[subject]; ok == true {
		return errors.New("There is already an action associated to the subject " + subject)
	}

	nt.actions[subject] = action

	if nt.conn != nil && nt.conn.Status() == nats.CONNECTED {
		if err := nt.subscribe(subject, action); err != nil {
			return err
		}
	}
	return nil
}

// HandleError specify the function to use for handling error returns by action
func (nt *Nats) HandleError(errHandler handler.ManageError) error {
	nt.errorHandler = errHandler
	return nil
}

// subscribeAll in nats for each actions associate to it.
func (nt *Nats) subscribeAll() error {
	// For each action stored in the Nats instance, subscribe to it
	// with the map key as subject.
	for subject, action := range nt.actions {
		if err := nt.subscribe(subject, action); err != nil {
			return err
		}
	}

	return nil
}

func (nt *Nats) subscribe(subject string, a handler.ListenerAction) error {
	subscribeType := "normal"
	subscribeHandler := func(msg *nats.Msg) {
		req := handler.Request{Data: msg.Data}
		resp, err := a.Fn(req)
		if err != nil {
			log.Errorf("Handling subject %s - %s", subject, err)

			if nt.errorHandler != nil {
				nt.conn.Publish(msg.Reply, nt.errorHandler(err))
				return
			}

			nt.conn.Publish(msg.Reply, []byte(err.Error()))
			return
		}

		nt.conn.Publish(msg.Reply, resp)
	}

	var err error

	for _, opt := range a.Opts {
		switch opt.Name {
		case NatsOptIsQueued:
			if opt.Value == true {
				subscribeType = "queued"
			}
		}
	}

	if subscribeType == "queued" {
		_, err = nt.conn.QueueSubscribe(subject, "job.workers", subscribeHandler)
	} else {
		_, err = nt.conn.Subscribe(subject, subscribeHandler)
	}

	if err != nil {
		log.Errorf("Error when adding subscription to nats - %s \n", err)
		return err
	}

	return nt.conn.Flush()
}

// Close the nats connection
func (nt *Nats) Close() error {
	if nt.conn != nil && nt.conn.Status() != nats.CONNECTED {
		return errors.New("Can not close nats connection because the status is not connected")
	}

	nt.closeChan <- true

	nt.conn.Close()

	// Reset close chan
	nt.closeChan = make(chan bool)

	nt.isClosed = true

	return nil
}

// // New returns a new instance of nats transporter which will connects to the urls
// // provided as parameter
// func New(connUrls string, opts []nats.Option) Nats {
// 	return Nats{
// 		connUrls:  connUrls,
// 		actions:   make(map[string]handler.ListenerAction),
// 		opts:      opts,
// 		closeChan: make(chan bool),
// 	}
// }

// New returns a new instance of nats transporter using an existing
// nats connection.
func New(conn *nats.Conn) Nats {
	return Nats{
		conn:      conn,
		actions:   make(map[string]handler.ListenerAction),
		closeChan: make(chan bool),
	}
}

// CreateConn create a nats connection using the natsURLS and specified nats options
func CreateConn(natsURLS string, opts []nats.Option) (*nats.Conn, error) {
	// Connect to NATS
	conn, err := nats.Connect(natsURLS, opts...)

	if err != nil {
		log.Error(err)

		return nil, err
	}

	return conn, nil
}

// DefaultConnOptions return a set of default options for the nats connection.
func DefaultConnOptions() (opts []nats.Option) {
	totalWait := 10 * time.Minute
	reconnectDelay := time.Second

	opts = append(opts, nats.ReconnectWait(reconnectDelay))
	opts = append(opts, nats.MaxReconnects(int(totalWait/reconnectDelay)))
	opts = append(opts, nats.DisconnectHandler(func(nc *nats.Conn) {
		log.Printf("Disconnected")
		log.Printf("Reconnecting for next %.0fm", totalWait.Minutes())
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		log.Printf("Reconnected [%s]", nc.ConnectedUrl())
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
		log.Printf("Close connection")
	}))
	return opts
}
