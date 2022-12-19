package quotrecreplay

import (
    "context"
    "fmt"
    "testing"
    "time"

    "github.com/stretchr/testify/assert"

    "github.com/chenjunpc2008/go-hbase/auto-gen/hbase"
    "github.com/chenjunpc2008/go-hbase/hbasepool"
    "github.com/chenjunpc2008/go-hbase/util"
)

const (
    cColumnMsg = "msg"

    cQualifierContent = "content"
)

func Test_row_scan(t *testing.T) {

    cnf := hbasepool.Config{Host: "hb-909", Port: 9090,
        MaxIdle: 2, MaxActive: 1000,
        IdleTimeout:     30 * time.Minute,
        MaxConnLifetime: 8 * time.Hour,
    }

    hbPool := hbasepool.NewPool(cnf)

    defer func() {
        hbPool.Close()
    }()

    // get hbase conn
    c, err := hbPool.Get()
    assert.Equal(t, nil, err)

    // return
    defer func() {
        hbPool.Put(c)
    }()

    byTblnm := []byte("full_YYYYMMDD")
    scaner := hbase.NewTScan()
    scanerid, err := c.Client.OpenScanner(context.Background(), byTblnm, scaner)
    assert.Equal(t, nil, err)

    res232, err := c.Client.GetScannerRows(context.Background(), scanerid, 100)
    assert.Equal(t, nil, err)

    for _, tr := range res232 {

        assert.NotEqual(t, nil, tr)

        clmns := tr.GetColumnValues()
        assert.NotEqual(t, nil, clmns)

        for _, v := range clmns {
            qclmn, err := util.DecodeHbaseTColumnValue(string(tr.Row), v)
            assert.Equal(t, nil, err)
            t.Log(qclmn)
        }

    }

    c.Client.CloseScanner(context.Background(), scanerid)
    t.FailNow()
}

func Test_row_scan_withFilter(t *testing.T) {

    cnf := hbasepool.Config{Host: "hb-909", Port: 9090,
        MaxIdle: 2, MaxActive: 1000,
        IdleTimeout:     30 * time.Minute,
        MaxConnLifetime: 8 * time.Hour,
    }

    hbPool := hbasepool.NewPool(cnf)

    defer func() {
        hbPool.Close()
    }()

    // get hbase conn
    c, err := hbPool.Get()
    assert.Equal(t, nil, err)

    // return
    defer func() {
        hbPool.Put(c)
    }()

    startRowkey := fmt.Sprintf("%d000_######", 1631090021)
    stopRowkey := fmt.Sprintf("%d000_######", 1631176868)

    byTblnm := []byte("full_YYYYMMDD")
    scanner := hbase.NewTScan()
    scanner.StartRow = []byte(startRowkey)
    scanner.StopRow = []byte(stopRowkey)

    tclm := hbase.NewTColumn()
    tclm.Family = []byte(cColumnMsg)
    tclm.Qualifier = []byte(cQualifierContent)
    tmpClms := make([]*hbase.TColumn, 0)
    tmpClms = append(tmpClms, tclm)
    scanner.Columns = tmpClms

    scanerid, err := c.Client.OpenScanner(context.Background(), byTblnm, scanner)
    assert.Equal(t, nil, err)

    res232, err := c.Client.GetScannerRows(context.Background(), scanerid, 100)
    assert.Equal(t, nil, err)

    for _, tr := range res232 {

        assert.NotEqual(t, nil, tr)

        clmns := tr.GetColumnValues()
        assert.NotEqual(t, nil, clmns)

        for _, v := range clmns {
            qclmn, err := util.DecodeHbaseTColumnValue(string(tr.Row), v)
            assert.Equal(t, nil, err)
            t.Log(qclmn)
        }

    }

    c.Client.CloseScanner(context.Background(), scanerid)
    t.FailNow()
}

func Test_row_scan_stock_withFilter(t *testing.T) {

    cnf := hbasepool.Config{Host: "hb-909", Port: 9090,
        MaxIdle: 2, MaxActive: 1000,
        IdleTimeout:     30 * time.Minute,
        MaxConnLifetime: 8 * time.Hour,
    }

    hbPool := hbasepool.NewPool(cnf)

    defer func() {
        hbPool.Close()
    }()

    // get hbase conn
    c, err := hbPool.Get()
    assert.Equal(t, nil, err)

    // return
    defer func() {
        hbPool.Put(c)
    }()

    startRowkey := fmt.Sprintf("%d000_######", 1631090021)
    stopRowkey := fmt.Sprintf("%d000_######", 1631176868)

    byTblnm := []byte("full_YYYYMMDD")
    scanner := hbase.NewTScan()
    scanner.StartRow = []byte(startRowkey)
    scanner.StopRow = []byte(stopRowkey)

    iLimit := new(int32)
    *iLimit = 3
    scanner.Limit = iLimit

    tclm := hbase.NewTColumn()
    tclm.Family = []byte(cColumnMsg)
    tclm.Qualifier = []byte(cQualifierContent)
    tmpClms := make([]*hbase.TColumn, 0)
    tmpClms = append(tmpClms, tclm)
    scanner.Columns = tmpClms

    scanner.FilterString = []byte("RowFilter(=,'substring:_10003413')")

    scanerid, err := c.Client.OpenScanner(context.Background(), byTblnm, scanner)
    assert.Equal(t, nil, err)

    res232, err := c.Client.GetScannerRows(context.Background(), scanerid, 100)
    assert.Equal(t, nil, err)

    for _, tr := range res232 {

        assert.NotEqual(t, nil, tr)

        clmns := tr.GetColumnValues()
        assert.NotEqual(t, nil, clmns)

        for _, v := range clmns {
            qclmn, err := util.DecodeHbaseTColumnValue(string(tr.Row), v)
            assert.Equal(t, nil, err)
            t.Log(qclmn)
        }

    }

    c.Client.CloseScanner(context.Background(), scanerid)
    t.FailNow()
}
