package com.bill.dao;

import com.bill.constans.Constants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;


/**
 * 实现基本功能：
 * 1.发布微博
 * 2.删除微博
 * 3.关注用户
 * 4.取关用户
 * 5.获取用户初始页面数据
 * 6.获取某个用户微博详情
 */
public class HBaseDao {

    public static void publishWeibo(String uid, String content) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        long ts = System.currentTimeMillis();

        String rowKey = uid + "_" + ts;

        Put contPut = new Put(Bytes.toBytes(rowKey));

        contPut.addColumn(Bytes.toBytes(Constants.CONTENT_TABLE_CF), Bytes.toBytes("content"), Bytes.toBytes(content));

        contTable.put(contPut);

        // 操作微博收件箱表
        Table relaTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));
        Get get = new Get(Bytes.toBytes(uid));
        get.addFamily(Bytes.toBytes(Constants.RELATION_TABLE_CF2));
        Result result = relaTable.get(get);

        ArrayList<Put> inboxPuts = new ArrayList<>();

        for (Cell cell : result.rawCells()) {
            Put inboxPut = new Put(CellUtil.cloneQualifier(cell));
            inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(uid), Bytes.toBytes(rowKey));
            inboxPuts.add(inboxPut);
        }
        if (inboxPuts.size() > 0) {
            Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
            inboxTable.put(inboxPuts);
            inboxTable.close();
        }
        contTable.close();
        relaTable.close();
        connection.close();
    }

    public static void addAttends(String uid, String... attends) throws IOException {
        if (attends.length <= 0) {
            System.out.println("请选择待关注的人！！！");
        }


        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //第一部分：操作用户关系表
        Table relaTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));

        ArrayList<Put> relaPuts = new ArrayList<>();

        Put uidPut = new Put(Bytes.toBytes(uid));

        for (String attend : attends) {
            uidPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF1), Bytes.toBytes(attend), Bytes.toBytes(attend));
            Put attendPut = new Put(Bytes.toBytes(attend));

            attendPut.addColumn(Bytes.toBytes(Constants.RELATION_TABLE_CF2), Bytes.toBytes(uid), Bytes.toBytes(uid));
            relaPuts.add(attendPut);
        }
        relaPuts.add(uidPut);
        relaTable.put(relaPuts);


        //第二部分：操作用户收件箱表
        //to be checked
        Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));

        Put inboxPut  = new Put(Bytes.toBytes(uid));

        for(String attend : attends) {

            Scan scan = new Scan(Bytes.toBytes(attend + "_"), Bytes.toBytes(attend + "|"));
            ResultScanner resultScanner = contTable.getScanner(scan);


            long ts = System.currentTimeMillis();

            for (Result result : resultScanner) {
                inboxPut.addColumn(Bytes.toBytes(Constants.INBOX_TABLE_CF), Bytes.toBytes(attend), ts++, result.getRow());
            }
        }
            if(!inboxPut.isEmpty()){
                Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));

                inboxTable.put(inboxPut);
                inboxTable.close();
            }
            relaTable.close();
            contTable.close();
            connection.close();



    }

    public static void deleteAttends(String uid, String... dels) throws IOException {

        if(dels.length <=0){
            System.out.println("请添加取关的用户！！！");
            return;
        }

        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //第一部分：操作用户关系表
        Table relaTable = connection.getTable(TableName.valueOf(Constants.RELATION_TABLE));

        ArrayList<Delete> relaDeletes = new ArrayList<>();

        Delete uidDelete = new Delete(Bytes.toBytes(uid));

        for (String del : dels) {
            uidDelete.addColumns(Bytes.toBytes(Constants.RELATION_TABLE_CF1),Bytes.toBytes(del));
            Delete delDelte = new Delete(Bytes.toBytes(uid));
            delDelte.addColumns(Bytes.toBytes(Constants.RELATION_TABLE_CF2),Bytes.toBytes(uid));
            relaDeletes.add(delDelte);
        }
        relaDeletes.add(uidDelete);
        relaTable.delete(relaDeletes);

        //第二部分：操作收件箱表
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
        Delete inboxDelete = new Delete(Bytes.toBytes(uid));

        for (String del : dels) {
            inboxDelete.addColumns(Bytes.toBytes(Constants.INBOX_TABLE_CF),Bytes.toBytes(del));
        }
        inboxTable.delete(inboxDelete);
        relaTable.close();
        inboxTable.close();
        connection.close();
    }


    // 获取某个人初始化页面数据
    public static void getInit(String uid) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Table contTable = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        Table inboxTable = connection.getTable(TableName.valueOf(Constants.INBOX_TABLE));
        Get inboxGet = new Get(Bytes.toBytes(uid));
        inboxGet.setMaxVersions();
        Result result = inboxTable.get(inboxGet);
        for (Cell cell : result.rawCells()) {
            Get contGet = new Get(CellUtil.cloneValue(cell));
            Result contResult = contTable.get(contGet);

            for (Cell contCell  : contResult.rawCells()) {
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(contCell))+
                  ",CF:" + Bytes.toString(CellUtil.cloneFamily(contCell)) +
                  ",CN:" + Bytes.toString(CellUtil.cloneQualifier(contCell)) +
                  ",Value:" +  Bytes.toString(CellUtil.cloneValue(contCell))
                );

            }
        }

        contTable.close();
        inboxTable.close();
        connection.close();

    }

    //获取某个人所有微博详情
    public static void getWeibo(String uid) throws IOException {
        Connection connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
        Table table = connection.getTable(TableName.valueOf(Constants.CONTENT_TABLE));
        Scan scan = new Scan();

        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator((uid + "_") ));

        scan.setFilter(rowFilter);
        ResultScanner resultScanner = table.getScanner(scan);

        for (Result result : resultScanner) {
            for (Cell contCell : result.rawCells()) {
                System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(contCell)) +
                        ",CF:" + Bytes.toString(CellUtil.cloneFamily(contCell)) +
                        ",CN:" + Bytes.toString(CellUtil.cloneQualifier(contCell)) +
                        ",Value:" + Bytes.toString(CellUtil.cloneValue(contCell))
                );
            }
        }

        table.close();
        connection.close();

    }

}
