package tnats

import (
	"errors"
	"time"

	"github.com/nanux-io/nanux/handler"

	"github.com/nats-io/go-nats"

	log "github.com/sirupsen/logrus"
)

/*----------------------------------------------------------------------------*\
  Options for actions handled by nats
\*----------------------------------------------------------------------------*/

// NatsOptIsQueued is the name for the option telling to nat that the action must be queued.
// It means that only one subscriber will respond to the request
const NatsOptIsQueued handler.OptName = "NATS_IS_QUEUED"

/*----------------------------------------------------------------------------*\
  tNats transporter
\*----------------------------------------------------------------------------*/

// Transporter define a tnats instance of transporter which resolve the `Listener`
// interface from nanux transporter package
type Transporter struct {
	conn *nats.Conn
	// The key corresponds to the subject associate to the action
	actions      map[string]handler.ListenerAction
	errorHandler handler.ManageError
	closeChan    chan bool
	isClosed     bool
}

// Listen initialize the connection to nats server and subscribe to
// the actions specified using Addhandler.
func (tn *Transporter) Listen() error {

	if tn.isClosed {
		errMsg := "The nats connection has been closed and can not be open again"
		log.Errorln(errMsg)

		return errors.New(errMsg)
	}

	if tn.conn == nil || tn.conn.Status() != nats.CONNECTED {
		errMsg := "The nats connection is either nil or do not have status connected"
		log.Errorln(errMsg)

		return errors.New(errMsg)
	}

	if err := tn.subscribeAll(); err != nil {
		tn.conn.Close()
		return err
	}

	tn.conn.Flush()

	if err := tn.conn.LastError(); err != nil {
		log.Fatal(err)
	}

	log.Infoln("Service listening with nats")

	// Wait to receive data from the close channel to close the connection
	<-tn.closeChan

	return nil
}

// HandleAction add the action and associate it to the subject. This action will be
// used when listen is called.
func (tn *Transporter) HandleAction(subject string, action handler.ListenerAction) error {
	// Check if an action is already associated to the subject
	if _, ok := tn.actions[subject]; ok == true {
		return errors.New("There is already an action associated to the subject " + subject)
	}

	tn.actions[subject] = action

	if tn.conn != nil && tn.conn.Status() == nats.CONNECTED {
		if err := tn.subscribe(subject, action); err != nil {
			return err
		}
	}
	return nil
}

// HandleError specify the function to use for handling error returns by action
func (tn *Transporter) HandleError(errHandler handler.ManageError) error {
	tn.errorHandler = errHandler
	return nil
}

// subscribeAll in nats for each actions associate to it.
func (tn *Transporter) subscribeAll() error {
	// For each action stored in the Nats instance, subscribe to it
	// with the map key as subject.
	for subject, action := range tn.actions {
		if err := tn.subscribe(subject, action); err != nil {
			return err
		}
	}

	return nil
}

func (tn *Transporter) subscribe(subject string, a handler.ListenerAction) error {
	subscribeType := "normal"
	subscribeHandler := func(msg *nats.Msg) {
		req := handler.Request{Data: msg.Data}
		resp, err := a.Fn(req)
		if err != nil {
			log.Errorf("Handling subject %s - %s", subject, err)

			if tn.errorHandler != nil {
				tn.conn.Publish(msg.Reply, tn.errorHandler(err))
				return
			}

			tn.conn.Publish(msg.Reply, []byte(err.Error()))
			return
		}

		tn.conn.Publish(msg.Reply, resp)
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
		_, err = tn.conn.QueueSubscribe(subject, "job.workers", subscribeHandler)
	} else {
		_, err = tn.conn.Subscribe(subject, subscribeHandler)
	}

	if err != nil {
		log.Errorf("Error when adding subscription to nats - %s \n", err)
		return err
	}

	return tn.conn.Flush()
}

// Close the nats connection
func (tn *Transporter) Close() error {
	if tn.conn != nil && tn.conn.Status() != nats.CONNECTED {
		return errors.New("Can not close nats connection because the status is not connected")
	}

	tn.closeChan <- true

	tn.conn.Close()

	// Reset close chan
	tn.closeChan = make(chan bool)

	tn.isClosed = true

	return nil
}

/*----------------------------------------------------------------------------*\
  Instantiation of tNats transporter
\*----------------------------------------------------------------------------*/

// New returns a new instance of nats transporter using an existing
// nats connection.
func New(nc *nats.Conn) Transporter {
	return Transporter{
		conn:      nc,
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
