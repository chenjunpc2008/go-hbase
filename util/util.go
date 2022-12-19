/*
Package util util methods
*/
package util

import "github.com/chenjunpc2008/go-hbase/auto-gen/hbase"

/*
HBValue hbase value
*/
type HBValue struct {
    RowKey    string
    Family    string
    Qualifier string
    Type      int8
    Value     string
}

func NewHBValue() *HBValue {
    return &HBValue{}
}

func DecodeHbaseTColumnValue(rowkey string, tv *hbase.TColumnValue) (*HBValue, error) {
    var (
        qv = NewHBValue()
    )

    qv.RowKey = rowkey

    qv.Family = string(tv.GetFamily())
    qv.Qualifier = string(tv.GetQualifier())
    qv.Type = tv.GetType()
    qv.Value = string(tv.GetValue())

    return qv, nil
}
