# TNATS

tNats is a Nats transporter to be used with [nanux](https://github.com/nanux-io/nanux)

## Usage

### Creation

To create a nats transporter there are 2 possibilities:

* with an existing Nats connection
* letting tnats creating a nats connection

#### With existing nats connection

```go

func creatingNatsTransporter() transporter.Listener {
  var nc *nats.Conn

  // TODO: create the nats conn by yourself

  return tnast.New(nc)
}

```

#### Using tnats for creating nats connection

```go

func creatingNatsTransporter() transporter.Listener {
  var err error
  var nc *nats.Conn
  var natsOpts []nats.Option

  natsOpts = tnats.DefaultConnOptions()

  nc, err = tnats.CreateConn(natsURLS, natsOpts)

  return tnast.New(nc)
}

```

### Actions

With nats transporter there is the availability to use queue subscriptions by 
setting it in the options of the Action (see following example)

```go
  handler.Action{
    Fn: fn,
    Opts: []handler.Opt{
      {Name: tnats.NatsOptIsQueued, Value: true},
    },
  }
```

## Development

Command to execute test: `go test -coverprofile=coverage.out -v &&  go tool cover -html=coverage.out -o coverage.html`
Command to execute test with check race condition: `go test -race -coverprofile=coverage.out -v &&  go tool cover -html=coverage.out -o coverage.html`

## Contributor

Thanks to [Nicolas Talle](https://github.com/nicolab) for the feedback.
