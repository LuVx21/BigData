package org.luvx.hbase.utils;

import javafx.util.Pair;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * @ClassName: org.luvx.hbase.utils
 * @Description:
 * @Author: Ren, Xie
 * @Date: 2019/11/13 10:46
 */
@Slf4j
public class HBaseUtils {
    public static boolean createTable(Connection connection, TableName tableName, String... columnFamilies) throws IOException {
        try (HBaseAdmin admin = (HBaseAdmin) connection.getAdmin()) {
            if (admin.tableExists(tableName)) {
                log.warn("表已存在: {}", tableName.getName());
                return false;
            }

            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
            Arrays.stream(columnFamilies).forEach(columnFamily -> {
                ColumnFamilyDescriptorBuilder builder1 = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));
                builder1.setMaxVersions(1);
                ColumnFamilyDescriptor familyDescriptor = builder1.build();
                builder.setColumnFamily(familyDescriptor);
            });
            admin.createTable(builder.build());
            log.info("create table:{} success!", tableName.getName());
            return true;
        }
    }

    public static boolean deleteTable(Connection connection, TableName tableName) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                log.info("delete table:{} success!", tableName.getName());
                return true;
            }
            log.warn("表不存在: {}", tableName.getName());
            return false;
        }
    }

    public static boolean disableTable(Connection connection, TableName tableName) throws IOException {
        try (Admin admin = connection.getAdmin()) {
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                log.info("disable table:{} success!", tableName.getName());
                return true;
            }
            log.warn("表不存在: {}", tableName.getName());
            return false;
        }
    }

    /**
     * 单行单列族单字段插入
     *
     * @param connection
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param data
     * @return
     * @throws IOException
     */
    public static boolean put(Connection connection, TableName tableName,
                              String rowKey, String columnFamily, String column, String data) throws IOException {
        try (Table table = connection.getTable(tableName)) {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
            table.put(put);
            log.info("put table:{} success!", Bytes.toString(tableName.getName()));
            return true;
        }
    }

    /**
     * 单行单列族多字段插入
     *
     * @param connection
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param pairList
     * @return
     * @throws IOException
     */
    public static boolean put(Connection connection, TableName tableName,
                              String rowKey, String columnFamily, List<Pair<String, String>> pairList) throws IOException {
        try (Table table = connection.getTable(tableName)) {
            Put put = new Put(Bytes.toBytes(rowKey));
            pairList.forEach(pair -> put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(pair.getKey()), Bytes.toBytes(pair.getValue())));
            table.put(put);
            log.info("put table:{} success!", Bytes.toString(tableName.getName()));
            return true;
        }
    }


    /**
     * 查询指定的最新版本数据
     * 列族和列为空 -> 查指定行
     * 列为空 -> 查指定行的指定列族
     *
     * @param connection
     * @param tableName  表名
     * @param rowKey     行(不可为空)
     * @param cf         列族(可为空)
     * @param qualifier  列(可为空)
     * @throws IOException
     */
    public static void get(Connection connection, TableName tableName, String rowKey, String cf, String qualifier) throws IOException {
        try (Table table = connection.getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(rowKey));
            if (!get.isCheckExistenceOnly()) {
                if (cf != null && cf.length() != 0) {
                    get.addFamily(Bytes.toBytes(cf));
                    if (qualifier != null && qualifier.length() != 0) {
                        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifier));
                    }
                }
            }
            Result r = table.get(get);
            print(r.getMap(), r.getRow());
        }
    }

    /**
     * 删除
     * 可删除行,列族,字段
     *
     * @param connection
     * @param tableName  表名
     * @param rowKey     行(不可为空)
     * @param cf         列族(可为空)
     * @param qualifier  列(可为空)
     * @return
     * @throws IOException
     */
    public static boolean delete(Connection connection, TableName tableName, String rowKey, String cf,
                                 String qualifier) throws IOException {
        try (Table table = connection.getTable(tableName)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            if (cf != null && cf.length() != 0) {
                delete.addFamily(Bytes.toBytes(cf));
                if (qualifier != null && qualifier.length() != 0) {
                    delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifier));
                }
            }
            table.delete(delete);
        }
        return true;
    }

    /**
     * 多行查询
     *
     * @param connection
     * @param tableName
     * @throws IOException
     */
    public static void scan(Connection connection, TableName tableName,
                            String startRowKey, String endRowKey, FilterList filterList) throws IOException {
        try (ResultScanner rs = getScanner(connection, tableName, startRowKey, endRowKey, filterList)) {
            for (Result r : rs) {
                print(r.getMap(), r.getRow());
            }
        }
    }

    /**
     * @param connection
     * @param tableName
     * @param startRowKey
     * @param endRowKey
     * @param filterList
     * @return
     * @throws IOException
     */
    public static ResultScanner getScanner(Connection connection, TableName tableName,
                                           String startRowKey, String endRowKey, FilterList filterList) throws IOException {
        try (Table table = connection.getTable(tableName)) {
            Scan scan = new Scan();
            if (startRowKey != null) {
                scan.withStartRow(Bytes.toBytes(startRowKey));
            }
            if (endRowKey != null) {
                scan.withStopRow(Bytes.toBytes(endRowKey));
            }
            if (filterList != null) {
                scan.setFilter(filterList);
            }
            return table.getScanner(scan);
        }
    }


    private static void print(NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowMap, byte[] b) {
        String rowKey = Bytes.toString(b);
        log.info("行: {}", rowKey);
        for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : rowMap.entrySet()) {
            String cfName = Bytes.toString(entry.getKey());
            log.info("    列族: {}", cfName);

            NavigableMap<byte[], NavigableMap<Long, byte[]>> cfMap = entry.getValue();
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> colMap : cfMap.entrySet()) {
                String columnName = Bytes.toString(colMap.getKey());
                NavigableMap<Long, byte[]> ma = colMap.getValue();
                for (Map.Entry<Long, byte[]> e : ma.entrySet()) {
                    log.info("        列: {} = {}, 版本: {}", columnName, Bytes.toString(e.getValue()), e.getKey());
                }
            }
        }
    }

    private static Row toBean(NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> rowMap, byte[] b) {
        Row row = new Row();
        row.setRowKey(Bytes.toString(b));

        // 循环列族
        for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : rowMap.entrySet()) {
            String cfName = Bytes.toString(entry.getKey());
            List<ColumnFamily> columnFamilies = row.getColumnFamilies();
            if (columnFamilies == null) {
                row.setColumnFamilies(columnFamilies = new ArrayList<>());
            }
            ColumnFamily cf = new ColumnFamily();
            cf.setCfName(cfName);
            columnFamilies.add(cf);

            NavigableMap<byte[], NavigableMap<Long, byte[]>> cfMap = entry.getValue();
            // 循环单个列族的所有列
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> colMap : cfMap.entrySet()) {
                List<Column> columns = cf.getColumns();
                if (columns == null) {
                    cf.setColumns(columns = new ArrayList<>());
                }

                String columnName = Bytes.toString(colMap.getKey());
                NavigableMap<Long, byte[]> ma = colMap.getValue();
                // 循环每个列的不同版本
                for (Map.Entry<Long, byte[]> e : ma.entrySet()) {
                    columns.add(
                            new Column(columnName, Bytes.toString(e.getValue()), e.getKey())
                    );
                }
            }
        }
        return row;
    }

    @Data
    public static class Row {
        private String             rowKey;
        private List<ColumnFamily> columnFamilies;
    }

    @Data
    public static class ColumnFamily {
        private String       cfName;
        private List<Column> columns;
    }

    @Data
    @AllArgsConstructor
    public static class Column {
        private String columnName;
        private String columnValue;
        private Long   timestamp;
    }
}
