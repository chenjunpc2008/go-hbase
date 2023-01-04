package util

import (
    "context"
    "errors"

    "github.com/chenjunpc2008/go-hbase/auto-gen/hbase"
)

/*
IsTableExists check if table exists
*/
func IsTableExists(client *hbase.THBaseServiceClient, tableName string) (bool, error) {

    var (
        ok  bool
        err error
    )

    // exists 'tb_test'

    ttb := &hbase.TTableName{Qualifier: []byte(tableName)}

    ok, err = client.TableExists(context.Background(), ttb)
    return ok, err
}

/*
CreateTable create table
*/
func CreateTable(client *hbase.THBaseServiceClient, tableName string, columns []string) error {

    if nil == columns || 0 == len(columns) {
        return errors.New("empty columns")
    }

    var (
        err error
    )

    ttn := &hbase.TTableName{Qualifier: []byte(tableName)}

    tdesc := hbase.NewTTableDescriptor()
    tdesc.TableName = ttn

    for _, v := range columns {
        clmn := hbase.NewTColumnFamilyDescriptor()
        clmn.Name = []byte(v)

        tdesc.Columns = append(tdesc.Columns, clmn)
    }

    containerStruct1 := hbase.NewTHBaseServiceCreateTableArgs()
    skeys := containerStruct1.SplitKeys
    err = client.CreateTable(context.Background(), tdesc, skeys)
    if nil != err {
        return err
    }

    return nil
}

/*
Put put a row to table
*/
func Put(client *hbase.THBaseServiceClient, tableName string,
    rowKey string, family string, qualifier string, value string) error {

    var (
        err error
    )

    argvalue0 := []byte(tableName)

    put := hbase.NewTPut()
    put.Row = []byte(rowKey)

    clmn := hbase.NewTColumnValue()
    clmn.Family = []byte(family)
    clmn.Qualifier = []byte(qualifier)
    clmn.Value = []byte(value)

    put.ColumnValues = append(put.ColumnValues, clmn)

    err = client.Put(context.Background(), argvalue0, put)
    return err
}
