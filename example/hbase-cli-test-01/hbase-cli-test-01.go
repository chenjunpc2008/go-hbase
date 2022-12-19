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
        host      string = "hb-909"
        port      int    = 9090
        portStr   string
        trans     thrift.TTransport
        err       error
        tableName string
        rowKey    string
        ok        bool
        argvalue0 []byte
        argvalue1 *hbase.TGet
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

    // check exists
    {
        tableName = "tb_test"
        rowKey = "10086"

        argvalue0 = []byte(tableName)

        argvalue1 = &hbase.TGet{Row: []byte(rowKey)}

        ok, err = client.Exists(context.Background(), argvalue0, argvalue1)
        if nil != err {
            fmt.Fprintln(os.Stderr, "Exists failed ", err)
            os.Exit(1)
        }

        if ok {
            fmt.Println(tableName, " ", rowKey, " exists")
        } else {
            fmt.Println(tableName, " ", rowKey, " not exists")
        }
    }

    // get
    {
        // put 'tb_test', '10086', 'name:idoall', 'yhfj'
        // put 'tb_test', '10086', 'name:hq', '"jfkdjfkdfd"'
        tableName = "tb_test"
        rowKey = "10086"

        argvalue0 = []byte(tableName)

        argvalue1 = &hbase.TGet{Row: []byte(rowKey)}

        res242, err := client.Get(context.Background(), argvalue0, argvalue1)
        if nil != err {
            fmt.Fprintln(os.Stderr, "Get failed ", err)
        } else {
            fmt.Println(tableName, " ", rowKey, " get ", string(res242.Row))
            clmns := res242.GetColumnValues()
            if nil == clmns {
                fmt.Println("nil ColumnValues")
            } else {
                for _, v := range clmns {
                    printfColumVl(v)
                }
            }
        }
    }

    {
        // put 'tb_test', '10043', 'name:it', '2454'
        tableName = "tb_test"
        rowKey = "10043"

        argvalue0 = []byte(tableName)

        argvalue1 = &hbase.TGet{Row: []byte(rowKey)}

        res242, err := client.Get(context.Background(), argvalue0, argvalue1)
        if nil != err {
            fmt.Fprintln(os.Stderr, "Get failed ", err)
        } else {
            fmt.Println(tableName, " ", rowKey, " get ", string(res242.Row))
            clmns := res242.GetColumnValues()
            if nil == clmns {
                fmt.Println("nil ColumnValues")
            } else {
                for _, v := range clmns {
                    printfColumVl(v)
                }
            }
        }
    }

    // exists
    {
        // exists 'tb_test'
        tableName = "tb_test"

        ttn := &hbase.TTableName{Qualifier: []byte("tb_test")}

        res232, err := client.TableExists(context.Background(), ttn)
        if nil != err {
            fmt.Fprintln(os.Stderr, "Exists failed ", err)
        } else {
            fmt.Println(tableName, " Exists ", res232)
        }
    }

    // create table
    // {
    // 	tableName = "tb_test_1"

    // 	ttn := &hbase.TTableName{Qualifier: []byte(tableName)}

    // 	desc391 := hbase.NewTTableDescriptor()
    // 	desc391.TableName = ttn

    // 	clmn391 := hbase.NewTColumnFamilyDescriptor()
    // 	clmn391.Name = []byte("msg1")

    // 	clmn392 := hbase.NewTColumnFamilyDescriptor()
    // 	clmn392.Name = []byte("msg2")

    // 	desc391.Columns = append(desc391.Columns, clmn391)
    // 	desc391.Columns = append(desc391.Columns, clmn392)

    // 	containerStruct1 := hbase.NewTHBaseServiceCreateTableArgs()
    // 	argvalue393 := containerStruct1.SplitKeys
    // 	err = client.CreateTable(context.Background(), desc391, argvalue393)
    // 	if nil != err {
    // 		fmt.Fprintln(os.Stderr, "CreateTable ", tableName, " failed ", err, " ", err.Error())
    // 	} else {
    // 		fmt.Println(tableName, "CreateTable ", tableName)
    // 	}
    // }

    // put data
    {
        // put 'tb_test', '10101', 'name:it', 'jd78634'
        tableName = "tb_test"
        rowKey = "10101"

        argvalue0 = []byte(tableName)

        put251 := hbase.NewTPut()
        put251.Row = []byte(rowKey)

        cl251 := hbase.NewTColumnValue()
        cl251.Family = []byte("name")
        cl251.Qualifier = []byte("it")
        cl251.Value = []byte("jd78634")

        put251.ColumnValues = append(put251.ColumnValues, cl251)

        err = client.Put(context.Background(), argvalue0, put251)
        if nil != err {
            fmt.Fprintln(os.Stderr, "Put failed ", err)
        } else {
            fmt.Println(tableName, " ", rowKey, " put success ")
        }
    }

    // put data multi Column
    {
        // put 'tb_test', '10101', 'name:it', 'jd78634'
        tableName = "tb_test"
        rowKey = "10102"

        argvalue0 = []byte(tableName)

        put251 := hbase.NewTPut()
        put251.Row = []byte(rowKey)

        {
            cl251 := hbase.NewTColumnValue()
            cl251.Family = []byte("name")
            cl251.Qualifier = []byte("it")
            cl251.Value = []byte("jd78635")

            put251.ColumnValues = append(put251.ColumnValues, cl251)
        }

        {
            cl251 := hbase.NewTColumnValue()
            cl251.Family = []byte("name")
            cl251.Qualifier = []byte("it2")
            cl251.Value = []byte("jd78636")

            put251.ColumnValues = append(put251.ColumnValues, cl251)
        }

        err = client.Put(context.Background(), argvalue0, put251)
        if nil != err {
            fmt.Fprintln(os.Stderr, "Put failed ", err)
        } else {
            fmt.Println(tableName, " ", rowKey, " put success ")
        }
    }

    // scan data
    {
        fmt.Println("==================\nscan data")
        argvalue0 = []byte(tableName)
        sc231 := hbase.NewTScan()
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

    // put data Multiple
    {
        // put 'tb_test', '10101', 'name:it', 'jd78634'
        tableName = "tb_test"

        argvalue0 = []byte(tableName)

        puts271 := make([]*hbase.TPut, 0)

        //
        rowKey271a1 := "10111"

        put271a1 := hbase.NewTPut()
        put271a1.Row = []byte(rowKey271a1)

        cl271a1 := hbase.NewTColumnValue()
        cl271a1.Family = []byte("name")
        cl271a1.Qualifier = []byte("it")
        cl271a1.Value = []byte("jd31001")

        put271a1.ColumnValues = append(put271a1.ColumnValues, cl271a1)

        puts271 = append(puts271, put271a1)

        //
        rowKey271a2 := "10112"

        put271a2 := hbase.NewTPut()
        put271a2.Row = []byte(rowKey271a2)

        cl271a2 := hbase.NewTColumnValue()
        cl271a2.Family = []byte("name")
        cl271a2.Qualifier = []byte("it")
        cl271a2.Value = []byte("jd31002")

        put271a2.ColumnValues = append(put271a2.ColumnValues, cl271a2)

        puts271 = append(puts271, put271a2)

        err = client.PutMultiple(context.Background(), argvalue0, puts271)
        if nil != err {
            fmt.Fprintln(os.Stderr, "Put failed ", err)
        } else {
            fmt.Println(tableName, " ", rowKey271a1, rowKey271a2, " putMultiple success ")
        }
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
