package tnats_test

import (
	"testing"

	"github.com/nats-io/go-nats"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var natsClient *nats.Conn
var natsURLS = "nats://gnatsd:4222"

func TestGo(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Nanux transporters Suite")
}

var _ = BeforeSuite(func() {
	var err error
	natsClient, err = nats.Connect(natsURLS)
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	natsClient.Flush()
	natsClient.Close()
})
