package com.time.geekbang;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HBaseOperation {
    private static final Logger logger = LoggerFactory.getLogger(HBaseOperation.class);

    private static Connection conns = null;

    private static Admin admin = null;

    static {
        try {
            // 建立链接
            Configuration configuration = HBaseConfiguration.create();
//            configuration.set("hbase.zookeeper.quorum","127.0.0.1");
            configuration.set("hbase.zookeeper.quorum","emr-worker-1,emr-worker-2,emr-header-1");
            configuration.set("hbase.zookeeper.property.clientPort","2181");
//            configuration.set("hbase.master","127.0.0.1:60000");
            conns = ConnectionFactory.createConnection(configuration);
            admin = conns.getAdmin();

        } catch (IOException e) {
            logger.error("init failed, ", e);
        }
    }

    /**
     * 建HBase表
     *
     * @param tableName tableName
     * @param columnFamilies columnFamilies
     */
    public static void createTable(String tableName, String ...columnFamilies) {
        if (tableName == null || columnFamilies.length < 1) {
            throw new IllegalArgumentException("tableName or columnFamilies is null!");
        }
        TableDescriptorBuilder tDescriptorBuilder
            = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        for (String columnFamily : columnFamilies) {
            ColumnFamilyDescriptor descriptor =
                    ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily)).build();
            tDescriptorBuilder.setColumnFamily(descriptor);
        }

        try {
            admin.createTable(tDescriptorBuilder.build());
            logger.info("createTable success, tableName: {}",tableName);
        } catch (IOException e) {
            logger.error("createTable failed, tableName: {}",tableName, e);
        }
    }

    /**
     * 删除表
     *
     * @param tableName tableName
     * @throws IOException io
     */
    public static void deleteTable(String tableName) throws IOException {
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
        logger.info("delete table success, table name si {}", tableName);
    }

    /**
     * 插入数据
     *
     * @param tableName tableName
     * @param rowKey rowKey
     * @param colFamily colFamily
     * @param colKey colKey
     * @param colValue colValue
     * @throws IOException io
     */
    public static void putData(String tableName, String rowKey,
        String colFamily, String colKey, String colValue) throws IOException {
        Table table = conns.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(colKey), Bytes.toBytes(colValue));
        table.put(put);
        table.close();
    }

    /**
     * 查询数据并且打日志
     *
     * @param tableName tableName
     * @param rowKey rowKey
     * @param colFamily colFamily
     * @param colKey colKey
     * @throws IOException io
     */
    public static void queryData(String tableName, String rowKey,
        String colFamily, String colKey) throws IOException {
        Table table = conns.getTable(TableName.valueOf(tableName));
        Get query = new Get(Bytes.toBytes(rowKey));
        if (colKey == null) {
            query.addFamily(Bytes.toBytes(colFamily));
        } else {
            query.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(colKey));
        }
        Result res = table.get(query);
        for (Cell cell : res.rawCells()) {
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            logger.info("Family is :{}, Qualifier is :{}, Value is :{}",family, qualifier, value);
        }

        table.close();
    }

    /**
     * 扫描表
     *
     * @param tableName tableName
     * @throws IOException io
     */
    public static void scanTable(String tableName) throws IOException {
        Table table = conns.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result res : resultScanner) {
            for (Cell cell : res.rawCells()) {
                String row = Bytes.toString(CellUtil.cloneRow(cell));
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                logger.info("Row:{}, Family is :{}, Qualifier is :{}, Value is :{}",row, family, qualifier, value);
            }
        }

    }

    /**
     * 删除表数据
     *
     * @param tableName tableName
     * @param rowKey rowKey
     * @param colFamily colFamily
     * @param colKey colKey
     * @throws IOException io
     */
    public static void deleteData(String tableName, String rowKey,
        String colFamily, String colKey) throws IOException {
        Table table = conns.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(colKey));

        table.delete(delete);
    }




    public static void main(String[] args) throws IOException {

        String tableName = "huster.chenbin.student";
        if (admin.tableExists(TableName.valueOf(tableName))) {
            deleteTable(tableName);
        }

        // 创建表
        createTable(tableName, "info", "score");

        // 构造数据
        Map<String, List<Long>> dataMap = new HashMap<>();
        dataMap.put("Tom", Arrays.asList(20210000000001L,1L,75L,82L));
        dataMap.put("Jerry", Arrays.asList(20210000000002L,1L,85L,82L));
        dataMap.put("Jack", Arrays.asList(20210000000003L,2L,85L,82L));
        dataMap.put("Rose", Arrays.asList(20210000000004L,2L,95L,82L));
        dataMap.put("Huster-chenbin", Arrays.asList(20210579030036L,3L,99L,100L));
        dataMap.put("chenbin", Arrays.asList(20210579030035L,3L,98L,99L));

        // 插入数据
        dataMap.forEach((k,v) -> {
            try {
                putData(tableName, k, "info", "student_id", v.get(0).toString());
                putData(tableName, k, "info", "class", v.get(1).toString());
                putData(tableName, k, "score", "understanding", v.get(2).toString());
                putData(tableName, k, "score", "programming", v.get(3).toString());
            } catch (Exception e) {
                logger.info("put data failed", e);
            }
        });

        // 查询
        logger.info("query data");
        queryData(tableName, "Huster-chenbin", "info", "student_id");
        queryData(tableName, "Huster-chenbin", "score", null);

        // 扫描全表
        logger.info("scan table");
        scanTable(tableName);

        // 删除数据
        logger.info("delete data");
        deleteData(tableName, "Huster-chenbin", "info", "student_id");
        deleteData(tableName, "Huster-chenbin", "score", "programming");

        // 查询
        logger.info("query data");
        queryData(tableName, "Huster-chenbin", "info", null);
        queryData(tableName, "Huster-chenbin", "score", null);
    }
}