/*
Package hbasepool hbase connection pool
*/
package hbasepool

import (
    "errors"
    "fmt"
    "sync"
    "time"

    "github.com/chenjunpc2008/go-hbase/auto-gen/hbase"
)

var _ = hbase.GoUnusedProtection__

/*
Config config
*/
type Config struct {
    Host string
    Port uint16

    // Maximum number of idle connections in the pool.
    MaxIdle int

    // Maximum number of connections allocated by the pool at a given time.
    // When zero, there is no limit on the number of connections in the pool.
    MaxActive int

    // Close connections after remaining idle for this duration. If the value
    // is zero, then idle connections are not closed. Applications should set
    // the timeout to a value less than the server's timeout.
    IdleTimeout time.Duration

    // Close connections older than this duration. If the value is zero, then
    // the pool does not close connections based on age.
    MaxConnLifetime time.Duration
}

/*
Pool hbase connection pool
*/
type Pool struct {
    cnf      Config
    mutex    sync.Mutex  // mutex protects the following fields
    closed   bool        // set to true when the pool is closed.
    active   int         // the number of open connections in the pool
    initOnce sync.Once   // the init ch once func
    idle     []*poolConn // idle connections
}

func NewPool(cnf Config) *Pool {
    return &Pool{cnf: cnf}
}

func (p *Pool) lazyInit() {
    p.initOnce.Do(func() {
        p.idle = make([]*poolConn, 0)
    })
}

/*
Get gets a connection. The application must close the returned connection.
*/
func (p *Pool) Get() (Conn, error) {

    var (
        c   Conn
        err error
    )

    p.lazyInit()

    p.mutex.Lock()
    defer p.mutex.Unlock()

    // Check for pool closed before create a new connection
    if p.closed {
        err = errors.New("wphbase: get on closed pool")
        return c, err
    }

    var (
        tnow = time.Now()
        tgap time.Duration
    )

    // cut stale connections of the idle list
    if 0 < p.cnf.IdleTimeout {
        if 0 != len(p.idle) {
            idleCleaned := make([]*poolConn, 0)

            for _, v := range p.idle {
                tgap = tnow.Sub(v.t)

                if tgap < p.cnf.IdleTimeout {
                    // not expired
                    idleCleaned = append(idleCleaned, v)
                } else {
                    // time out, close the connection
                    v.c.Close()
                    p.active--
                }
            }

            // swap
            p.idle = idleCleaned
        }
    }

    // pick from idle list
    for 0 != len(p.idle) {
        // pick the first one
        c = p.idle[0].c
        p.idle = p.idle[1:]

        if 0 > p.cnf.MaxConnLifetime {
            tgap = tnow.Sub(c.CreateTime)
            if tgap < p.cnf.MaxConnLifetime {
                // not expired
                return c, nil
            }

            // time out, close the connection
            c.Close()
            p.active--
        }
    }

    // Handle limit
    if 0 > p.cnf.MaxActive && p.active >= p.cnf.MaxActive {
        sErrMsg := fmt.Sprintf("wphbase: reach MaxActive:%d", p.cnf.MaxActive)
        err = errors.New(sErrMsg)
        return c, err
    }

    c, err = NewConn(p.cnf.Host, p.cnf.Port)
    if nil != err {
        return c, err
    }

    p.active++

    return c, nil
}

/*
Put put connection back
*/
func (p *Pool) Put(c Conn) {
    p.mutex.Lock()
    defer p.mutex.Unlock()

    if c.Closed {
        // this connection is err
        p.active--
        return
    }

    // check if pool is closed
    if p.closed {
        return
    }

    if 0 != p.cnf.MaxIdle {
        if len(p.idle) >= p.cnf.MaxIdle {
            // reach limit
            c.Close()
            return
        }
    }

    pc := &poolConn{c: c, t: time.Now()}
    p.idle = append(p.idle, pc)
}

/*
Close close connection
*/
func (p *Pool) Close() {

    p.mutex.Lock()
    defer p.mutex.Unlock()

    if nil != p.idle {
        for _, v := range p.idle {
            v.c.Close()
        }
    }
    p.idle = nil
    p.active = 0

    p.closed = true
}

type poolConn struct {
    c Conn
    t time.Time
}
