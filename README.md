# go-tcp
Go Hbase library: <https://github.com/chenjunpc2008/go-hbase>

# Usage

## hbasePool
---
for example: ```example/pool-test```

1. Use config parameters to create a new pool.

2. Get() a conn object from pool handle, and don't forget to put it back(use Put()) after you done, otherwise the pool will run out of connections.
    ```go
    cnf := hbasepool.Config{Host: "hb-909", Port: 9090,
            MaxIdle: 2, 
            MaxActive: 1000,
            IdleTimeout:     30 * time.Minute,
            MaxConnLifetime: 8 * time.Hour,
        }

    hbPool := hbasepool.NewPool(cnf)

    // get hbase conn
    c, err := hbPool.Get()
    assert.Equal(t, nil, err)

    // return
    defer func() {
        hbPool.Put(c)
    }()

    // do some work below
    // ...
    ```

3. Don't forget to Close() the pool handle before close your application.
   ```go
   hbPool.Close()
   ```
