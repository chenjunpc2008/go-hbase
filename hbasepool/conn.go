package hbasepool

import (
    "fmt"
    "net"
    "time"

    "github.com/apache/thrift/lib/go/thrift"

    "github.com/chenjunpc2008/go-hbase/auto-gen/hbase"
)

/*
Conn hbase conn
*/
type Conn struct {
    Closed     bool
    CreateTime time.Time
    Trans      thrift.TTransport
    Client     *hbase.THBaseServiceClient
}

/*
NewConn new hbase conn
*/
func NewConn(host string, port uint16) (Conn, error) {

    var (
        portStr string
        trans   thrift.TTransport
        client  *hbase.THBaseServiceClient
        err     error
        c       = Conn{Closed: true}
    )

    portStr = fmt.Sprint(port)

    conf := &thrift.TConfiguration{
        ConnectTimeout: 3 * time.Second,
        SocketTimeout:  200 * time.Second,

        MaxFrameSize: 1024 * 1024 * 256,

        TBinaryStrictRead:  thrift.BoolPtr(true),
        TBinaryStrictWrite: thrift.BoolPtr(true),
    }

    trans = thrift.NewTSocketConf(net.JoinHostPort(host, portStr), conf)

    protoFactory := thrift.NewTBinaryProtocolFactoryConf(conf)

    iprot := protoFactory.GetProtocol(trans)
    oprot := protoFactory.GetProtocol(trans)
    client = hbase.NewTHBaseServiceClient(thrift.NewTStandardClient(iprot, oprot))
    err = trans.Open()
    if nil != err {
        trans.Close()
        return c, err
    }

    c.Closed = false
    c.Trans = trans
    c.Client = client
    c.CreateTime = time.Now()

    return c, nil
}

/*
Close close
*/
func (c *Conn) Close() {

    if !c.Closed {
        if nil != c.Trans {
            c.Trans.Close()
        }
    }

    c.Client = nil
    c.Closed = true
}
