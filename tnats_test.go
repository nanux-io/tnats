package tnats_test

import (
	"errors"
	"time"

	"github.com/nats-io/go-nats"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/nanux-io/nanux/handler"
	. "github.com/nanux-io/nanux/transporter"
	. "github.com/nanux-io/tnats"
)

var _ = Describe("Nats transporter", func() {
	var conn *nats.Conn
	var nt Transporter

	res := []byte("action called")
	sub := "testSub"
	subErr := "testSubErr"

	It("should create a nats connection", func() {
		var err error
		conn, err = CreateConn(natsURLS, DefaultConnOptions())
		Expect(err).ToNot(HaveOccurred())

		nt = New(conn)
	})

	It("should satisfy Listener interface", func() {
		var i interface{} = &nt
		_, ok := i.(Listener)
		Expect(ok).To(Equal(true))
	})

	It("should allow to add actions before listening", func() {
		fn1 := func(req handler.Request) ([]byte, error) {
			return res, nil
		}

		action := handler.ListenerAction{Fn: fn1}

		err := nt.HandleAction(sub, action)
		Expect(err).ShouldNot(HaveOccurred())

		// add subscription which will generate an error. It will be used in an other test.
		fn2 := func(req handler.Request) ([]byte, error) {
			return nil, errors.New("Error in action")
		}

		err = nt.HandleAction(subErr, handler.ListenerAction{Fn: fn2})
		Expect(err).ShouldNot(HaveOccurred())

	})

	It("should allow action with queued option", func() {
		fn := func(req handler.Request) ([]byte, error) {
			return res, nil
		}

		action := handler.ListenerAction{
			Fn:   fn,
			Opts: []handler.Opt{{Name: NatsOptIsQueued, Value: true}},
		}

		err := nt.HandleAction("queuedsub", action)
		Expect(err).ShouldNot(HaveOccurred())
	})

	It("should throw error if adding several actions for same subject", func() {
		fn := func(req handler.Request) ([]byte, error) { return nil, nil }

		action := handler.ListenerAction{Fn: fn}

		nt.HandleAction("sameSub", action)
		err := nt.HandleAction("sameSub", action)
		Expect(err).To(BeAssignableToTypeOf(errors.New("")))
	})

	It("should start to listen", func() {
		go func() {
			err := nt.Listen()
			Expect(err).ShouldNot(HaveOccurred())
		}()
		time.Sleep(100 * time.Millisecond)
	})

	It("should respond to subscribed topic", func() {
		// Set nats into listening

		// FIXME: check directly with connection status to know when it is connected
		// Wait for connection to be established
		msg, err := natsClient.Request(sub, nil, time.Second)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(msg.Data).To(Equal(res))
	})

	It("should reply with error from action", func() {
		msg, err := natsClient.Request(subErr, nil, time.Second)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(msg.Data).To(Equal([]byte("Error in action")))
	})

	It("should allow to add error handler", func() {
		// add error manager
		errorHandler := func(error) []byte { return []byte("Error managed") }
		err := nt.HandleError(errorHandler)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should manage when error occured in action", func() {
		msg, err := natsClient.Request(subErr, nil, time.Second)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(msg.Data).To(Equal([]byte("Error managed")))
	})

	It("should close the connection", func() {
		err := nt.Close()
		Expect(err).ToNot(HaveOccurred())
	})

	It("should have error when closing unconnected connection", func() {
		err := nt.Close()
		Expect(err.Error()).To(Equal("Can not close nats connection because the status is not connected"))
	})

	It("should raise an error when try to listen an already close connection", func() {
		err := nt.Listen()

		Expect(err).To(HaveOccurred())
	})

	It("should fail to create connection with wrong urls", func() {
		_, err := CreateConn("wrong urls", DefaultConnOptions())

		Expect(err).To(HaveOccurred())
	})

	It("should fail to listen when the connection is not open", func() {
		// Create a nats connection
		conn, err := CreateConn(natsURLS, DefaultConnOptions())
		Expect(err).ToNot(HaveOccurred())

		// Close the connection before use it with nats transporter
		conn.Close()

		nt := New(conn)
		err = nt.Listen()

		Expect(err).To(HaveOccurred())

	})
})
