package com.zhifa.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;


public class HbaseApi {

    private static Connection connection = null;
    private static Admin admin = null;
    public static Connection getCon() throws Exception {
        System.out.println("开始执行");
        //初始化Hbase连接
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "47.100.45.101");

        //加载配置文件
        //configuration.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));
        //configuration.addResource(new Path(System.getProperty("C:\\Users\\jack\\Desktop\\hadoop\\hadoop\\src\\main\\resources\\hbase-site.xml")));
       // configuration.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));
        //configuration.set("hbase.master", "zhifa:16010");//hbase的0系列默认端口是60000;1系列默认端口16010
        Connection connection = ConnectionFactory.createConnection(configuration);
        System.out.println("==========连接成功！==========");
        return connection;
    }
    static {
        try {
            connection = getCon();
            admin = connection.getAdmin();
            System.out.println("==========admin获取成功！==========");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static boolean isTableExist(String table) throws Exception {
        boolean exists = admin.tableExists(TableName.valueOf(table));
        return exists;
    }
    /**
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
       //System.setProperty("hadoop.home.dir", "C:\\Users\\jack\\Desktop\\hadoop\\hadoop\\src\\main\\resources");
        boolean tableExist = isTableExist("student");
        System.out.println(tableExist);
        System.out.println("==========操作结束==========");
        admin.close();
        connection.close();

    }
}
