package com.bill.test;

import com.bill.constans.Constants;
import com.bill.dao.HBaseDao;
import com.bill.utils.HBaseUtil;

import java.io.IOException;



public class TestWeibo {

    public static void init(){
        //创建命名空间
        try {
            HBaseUtil.createNamespace(Constants.NAMESPACE);
            //创建微博内容表
            HBaseUtil.createTable(Constants.CONTENT_TABLE,Constants.CONTENT_TABLE_VERSIONS,Constants.CONTENT_TABLE_CF);
            //创建用户关系表
            HBaseUtil.createTable(Constants.RELATION_TABLE,Constants.RELATION_TABLE_VERSIONS,Constants.RELATION_TABLE_CF1,Constants.RELATION_TABLE_CF2);
            //创建收件箱表
            HBaseUtil.createTable(Constants.INBOX_TABLE,Constants.INBOX_TABLE_VERSIONS,Constants.INBOX_TABLE_CF);

        } catch (IOException e) {
            e.printStackTrace();
        }



    }


    public static void main(String[] args) throws IOException, InterruptedException {
        init();

        //1001发布微博
        HBaseDao.publishWeibo("1001","赶紧放假吧！！！");
        //1002关注1001和1003
        HBaseDao.addAttends("1002","1001","1003");
        //获取1002初始化页面
        HBaseDao.getInit("1002");
        System.out.println("*************1111*************");
        //1003发布3条微博，同时1001发布2条微博
        HBaseDao.publishWeibo("1003","谁说赶紧放假！！！");
        Thread.sleep(10);
        HBaseDao.publishWeibo("1001","我没说话！！！");
        Thread.sleep(10);
        HBaseDao.publishWeibo("1003","那谁说！！！");
        Thread.sleep(10);
        HBaseDao.publishWeibo("1001","反正有人放假了！！！");
        Thread.sleep(10);
        HBaseDao.publishWeibo("1003","你们随便。。！！！");

        //获取1002初始化页面
        HBaseDao.getInit("1002");
        System.out.println("*************2222*************");
        //1002取关1003
        HBaseDao.deleteAttends("1002","1003");

        //获取1002初始化页面
        HBaseDao.getInit("1002");
        System.out.println("*************3333*************");
        //1002再次关注1003
        HBaseDao.addAttends("1002","1003");

        //获取1002初始化页面
        HBaseDao.getInit("1002");
        System.out.println("*************4444*************");

        //获取1001初始化页面
        HBaseDao.getWeibo("1001");
    }
}
