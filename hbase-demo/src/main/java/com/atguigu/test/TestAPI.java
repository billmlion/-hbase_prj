package com.atguigu.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class TestAPI {

    public static Configuration conf = null;
    public static HBaseAdmin admin = null;
    public static Connection connection = null;

    static {
//使用 HBaseConfiguration 的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hbase-docker");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = (HBaseAdmin) connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static boolean isTableExist(String tableName) throws
            IOException {
        //在 HBase 中管理、访问表需要先创建 HBaseAdmin 对象
//        Connection connection = ConnectionFactory.createConnection(conf);
//        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
//        HBaseAdmin admin = new HBaseAdmin(conf);
        return admin.tableExists(tableName);
    }


    public static void createTable(String tableName, String...
            columnFamily) throws
            IOException {
//        HBaseAdmin admin = new HBaseAdmin(conf);
        //判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println("表" + tableName + "已存在");
            //System.exit(0);
        } else {
            //创建表属性对象,表名需要转字节
            HTableDescriptor descriptor = new
                    HTableDescriptor(TableName.valueOf(tableName));
            //创建多个列族
            for (String cf : columnFamily) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            //根据对表的配置，创建表
            admin.createTable(descriptor);
            System.out.println("表" + tableName + "创建成功！");
        }
    }


    public static void dropTable(String tableName) throws
            IOException {
//        HBaseAdmin admin = new HBaseAdmin(conf);
        if (isTableExist(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("表" + tableName + "删除成功！");
        } else {
            System.out.println("表" + tableName + "不存在！");
        }
    }

    public static void createNameSpace(String nameSpace) {
//        HBaseAdmin admin = null;
        try {
//            admin = new HBaseAdmin(conf);
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
            admin.createNamespace(namespaceDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void putData(String tableName, String rowKey, String
            columnFamily, String
                                       column, String value) throws IOException {
        //创建 table 对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //向表中插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        //向 Put 对象中组装数据
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
                Bytes.toBytes(value));
        table.put(put);
        table.close();
        System.out.println("插入数据成功");
    }


    public static void close() throws IOException {
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }

    }


    public static void getAllRows(String tableName) throws IOException {
        HTable hTable = new HTable(conf, tableName);
//得到用于扫描 region 的对象
        Scan scan = new Scan();
//使用 HTable 得到 resultcanner 实现类的对象
        ResultScanner resultScanner = hTable.getScanner(scan);
        for (Result result : resultScanner) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
//得到 rowkey
                System.out.println(" 行 键 :" +
                        Bytes.toString(CellUtil.cloneRow(cell)));
//得到列族
                System.out.println(" 列 族 " +
                        Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println(" 列 :" +
                        Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println(" 值 :" +
                        Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    public static void getData(String tableName, String rowKey,String cf, String cn) throws
            IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
//get.setMaxVersions();显示所有版本
        //get.setTimeStamp();显示指定时间戳的版本
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println(" 行 键 :" +
                    Bytes.toString(result.getRow()));
            System.out.println(" 列 族 " +
                    Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println(" 列 :" +
                    Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println(" 值 :" +
                    Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳:" + cell.getTimestamp());
        }
        table.close();
    }


    public static void getRow(String tableName, String rowKey) throws
            IOException {
        HTable table = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
//get.setMaxVersions();显示所有版本
        //get.setTimeStamp();显示指定时间戳的版本
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println(" 行 键 :" +
                    Bytes.toString(result.getRow()));
            System.out.println(" 列 族 " +
                    Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println(" 列 :" +
                    Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println(" 值 :" +
                    Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳:" + cell.getTimestamp());
        }
    }





    public static void main(String[] args) throws IOException {
        System.out.println(isTableExist("stu5"));
        createTable("stu5", "info1", "info2");
//        dropTable("stu5");

//      createNameSpace("0408");
//        putData("stu5", "1001", "info1", "name", "zhangsan");
        getData("stu5","1001","","");
        close();
    }
}



