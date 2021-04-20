package com.atguigu.test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
public class HBaseUtil {
    private static ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();

//    private static Connection conn = null;

    private HBaseUtil() {
    }

    /**
     * 获取连接对象
     * @return
     */
    public static void makeHBaseConnection(String hostName,String post) throws IOException {
//        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", hostName);
//        conf.set("hbase.zookeeper.property.clientPort", post);
//        conn = ConnectionFactory.createConnection(conf);

        Connection conn = connHolder.get();
        if (conn == null){
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", hostName);
            conf.set("hbase.zookeeper.property.clientPort", post);
            conn = ConnectionFactory.createConnection(conf);
            connHolder.set(conn);
        }

    }

    /**
     * 生成分区键，例如3个分区需要两个分区键
     * @param regionCount
     * @return
     */
    public static byte[][] genRegionKeys(int regionCount){
        byte[][] bs = new byte[regionCount-1][];

        for (int i = 0; i < regionCount - 1; i++) {
            bs[i] = Bytes.toBytes(i+"|");
        }

        return bs;
    }

    /**
     * 生成分区号
     * @param rowKey 根据rowKey生成分区号
     * @param regionCount 分区数量
     */
    public static String genRegionNum(String rowKey,int regionCount){
        int regionNum;
        int hash = rowKey.hashCode();
        if (regionCount > 0 && (regionCount & (regionCount -1)) == 0){
            //2的n次方
            regionNum = hash & (regionCount - 1);
        }else {
            regionNum = hash & (regionCount);
        }

        return regionNum + "_" + rowKey;
    }


    /**
     * 插入数据
     * @param tableName
     * @param rowKey
     * @param family
     * @param value
     */
    public static void insertData(String tableName,String rowKey,String family,String column,String value) throws IOException {
        Connection conn = connHolder.get();
        Table table = conn.getTable(TableName.valueOf(tableName));

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(value));

        table.put(put);
        table.close();
    }

    /**
     * 关闭连接
     */
    public static void close() throws IOException {
        Connection conn = connHolder.get();
        if (conn != null){
            conn.close();
            connHolder.remove();
        }
    }
}
