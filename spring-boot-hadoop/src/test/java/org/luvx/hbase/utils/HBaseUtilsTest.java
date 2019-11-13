package org.luvx.hbase.utils;

import javafx.util.Pair;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @ClassName: org.luvx.hbase.utils
 * @Description:
 * @Author: Ren, Xie
 * @Date: 2019/11/13 10:48
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class HBaseUtilsTest {
    Connection connection = HBaseConnectionUtils.getConnection();
    TableName  tableName  = TableName.valueOf("t_demo");

    @Test
    void createTableTest() throws IOException {
        HBaseUtils.createTable(connection, tableName, "cf1", "cf2");
    }

    @Test
    void deleteTableTest() throws IOException {
        HBaseUtils.deleteTable(connection, tableName);
    }

    @Test
    void disableTableTest() throws IOException {
        HBaseUtils.disableTable(connection, tableName);
    }

    @Test
    void putTest() throws IOException {
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
    void scanTest() throws IOException {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter nameFilter = new SingleColumnValueFilter(Bytes.toBytes("cf1"),
                Bytes.toBytes("user_name"), CompareOperator.EQUAL, Bytes.toBytes("alice"));
        filterList.addFilter(nameFilter);

        HBaseUtils.scan(connection, tableName, "user_1", "user_3", filterList);
    }

    @Test
    void getTest() throws IOException {
        HBaseUtils.get(connection, tableName, "user_1", "cf1", "age");
    }

    @Test
    void deleteTest() throws IOException {
        HBaseUtils.delete(connection, tableName, "user_1", "cf2", "user_name");
    }
}