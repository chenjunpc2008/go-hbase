package main

import (
    "context"
    "fmt"
    "net"
    "os"
    "strconv"
    "time"

    "github.com/apache/thrift/lib/go/thrift"

    "github.com/chenjunpc2008/go-hbase/auto-gen/hbase"
)

var _ = hbase.GoUnusedProtection__

func main() {
    var (
        host      string = "hb-21-152"
        port      int    = 9090
        portStr   string
        trans     thrift.TTransport
        err       error
        tableName string
        argvalue0 []byte
    )

    portStr = fmt.Sprint(port)

    conf := &thrift.TConfiguration{
        ConnectTimeout: time.Second,
        SocketTimeout:  time.Second,

        MaxFrameSize: 1024 * 1024 * 256,

        TBinaryStrictRead:  thrift.BoolPtr(true),
        TBinaryStrictWrite: thrift.BoolPtr(true),
    }

    trans = thrift.NewTSocketConf(net.JoinHostPort(host, portStr), conf)

    defer trans.Close()

    protoFactory := thrift.NewTBinaryProtocolFactoryConf(conf)

    iprot := protoFactory.GetProtocol(trans)
    oprot := protoFactory.GetProtocol(trans)
    client := hbase.NewTHBaseServiceClient(thrift.NewTStandardClient(iprot, oprot))
    err = trans.Open()
    if nil != err {
        fmt.Fprintln(os.Stderr, "Error opening socket to ", host, ":", port, " ", err)
        os.Exit(1)
    }

    // exists
    {
        // exists 'tb_test'
        tableName = "full_20211101"

        ttn := &hbase.TTableName{Qualifier: []byte(tableName)}

        res232, err := client.TableExists(context.Background(), ttn)
        if nil != err {
            fmt.Fprintln(os.Stderr, "Exists failed ", err)
        } else {
            fmt.Println(tableName, " Exists ", res232)
        }
    }

    const (
        cColumnInfo = "info"
        cColumnMsg  = "msg"

        cQualifierStockID = "stockID"
        cQualifierContent = "content"
    )

    // scan data
    {
        tableName = "full_20211101"

        fmt.Println("==================\nscan data")

        lastProcTime := 1635747124
        nextProcTime := 1635750000

        startRowkey := fmt.Sprintf("%d000_######", lastProcTime)
        stopRowkey := fmt.Sprintf("%d000_######", nextProcTime)

        argvalue0 = []byte(tableName)
        sc231 := hbase.NewTScan()
        sc231.StartRow = []byte(startRowkey)
        sc231.StopRow = []byte(stopRowkey)

        iLimit := new(int32)
        *iLimit = 3
        sc231.Limit = iLimit

        tclm := hbase.NewTColumn()
        tclm.Family = []byte(cColumnMsg)
        tclm.Qualifier = []byte(cQualifierContent)
        tmpClms := make([]*hbase.TColumn, 0)
        tmpClms = append(tmpClms, tclm)
        sc231.Columns = tmpClms

        sc231.FilterString = []byte("RowFilter(=,'substring:_10003413')")

        scanerid, err := client.OpenScanner(context.Background(), argvalue0, sc231)
        if nil != err {
            fmt.Fprintln(os.Stderr, "Put failed ", err)
        }

        res232, err := client.GetScannerRows(context.Background(), scanerid, 100)
        if nil != err {
            fmt.Fprintln(os.Stderr, "GetScannerRows failed ", err)
        }

        for _, v := range res232 {
            fmt.Println(tableName, " get ", string(v.Row))
            clmns := v.GetColumnValues()
            if nil == clmns {
                fmt.Println("nil ColumnValues")
            } else {
                for _, v := range clmns {
                    printfColumVl(v)
                }
            }
        }

        client.CloseScanner(context.Background(), scanerid)
    }
}

func printfColumVl(ele *hbase.TColumnValue) {
    if nil == ele {
        fmt.Println("nil TColumnValue")
        return
    }

    fmt.Println(ele.String())

    var (
        sType  string
        sValue string
        iValue int
    )

    iType := ele.GetType()
    switch iType {

    case thrift.BYTE:
        sType = "thrift.BYTE"
        sValue = string(ele.GetValue())

    case thrift.I16:
        sType = "thrift.I16"
        sValue = fmt.Sprintf("%d", ele.GetValue())
        iTmp, err := strconv.ParseInt(sValue, 10, 16)
        if nil != err {
            fmt.Println("strconv.ParseInt failed", err)
        }

        iValue = int(iTmp)

    case thrift.I32:
        sType = "thrift.I32"
        sValue = fmt.Sprintf("%d", ele.GetValue())
        iTmp, err := strconv.ParseInt(sValue, 10, 32)
        if nil != err {
            fmt.Println("strconv.ParseInt failed", err)
        }

        iValue = int(iTmp)

    case thrift.I64:
        sType = "thrift.I64"
        sValue = fmt.Sprintf("%d", ele.GetValue())
        iTmp, err := strconv.ParseInt(sValue, 10, 64)
        if nil != err {
            fmt.Println("strconv.ParseInt failed", err)
        }

        iValue = int(iTmp)

    case thrift.STRING:
        sType = "thrift.STRING"
        sValue = string(ele.GetValue())

    default:
        sType = "thrift.default"
        sValue = string(ele.GetValue())
    }

    fmt.Printf("Family:%s, Qualifier:%s, Type:%d-%s, Value:%s, int:%d,  Timestamp:%d \n", string(ele.GetFamily()), string(ele.GetQualifier()),
        iType, sType, sValue, iValue, ele.GetTimestamp())
}
