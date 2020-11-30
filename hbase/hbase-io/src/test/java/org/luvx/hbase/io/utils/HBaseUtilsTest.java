package org.luvx.hbase.io.utils;

import javafx.util.Pair;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @author Ren, Xie
 */
public class HBaseUtilsTest {
    Connection connection = HBaseConnectionUtils.getConnection();
    TableName  tableName  = TableName.valueOf("t_demo");

    @Test
    public void createTableTest() throws IOException {
        HBaseUtils.createTable(connection, tableName, "cf1", "cf2");
    }

    @Test
    public void deleteTableTest() throws IOException {
        HBaseUtils.deleteTable(connection, tableName);
    }

    @Test
    public void disableTableTest() throws IOException {
        HBaseUtils.disableTable(connection, tableName);
    }

    @Test
    public void putTest() throws IOException {
        String rowKey = "user_1";
        HBaseUtils.put(connection, tableName, rowKey, "cf1", "user_name", "F.LuVx");
        HBaseUtils.put(connection, tableName, rowKey, "cf1", "password", "1121");
        HBaseUtils.put(connection, tableName, rowKey, "cf1", "age", "18");

        List<Pair<String, String>> pairs = Arrays.asList(new Pair<>("user_name", "alice"),
                new Pair<>("password", "0000"),
                new Pair<>("age", "22"));
        HBaseUtils.put(connection, tableName, "user_2", "cf1", pairs);
    }

    @Test
    public void scanTest() throws IOException {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter nameFilter = new SingleColumnValueFilter(Bytes.toBytes("cf1"),
                Bytes.toBytes("user_name"), CompareOperator.EQUAL, Bytes.toBytes("alice"));
        filterList.addFilter(nameFilter);

        HBaseUtils.scan(connection, tableName, "user_1", "user_3", filterList);
    }

    @Test
    public void getTest() throws IOException {
        HBaseUtils.get(connection, tableName, "user_1", "cf1", "age");
    }

    @Test
    public void deleteTest() throws IOException {
        HBaseUtils.delete(connection, tableName, "user_1", "cf2", "user_name");
    }
}