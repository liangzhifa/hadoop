package com.zhifa;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    String uri=null;
    Configuration conf=null;
    FileSystem fs=null;
    @Before
    public  void before() throws IOException, InterruptedException {
        System.setProperty("hadoop.home.dir", "C:\\Users\\jack\\Desktop\\hadoop\\hadoop\\src\\main\\resources");
       // System.setProperty("dfs.replication","1");

        uri = "hdfs://zhifa:9000/";
        conf = new Configuration();
        conf.set("dfs.replication", "1");
        conf.set("dfs.client.use.datanode.hostname", "true");
        //conf.set("dfs.blocksize", "20480");
        fs = FileSystem.get(URI.create(uri),conf,"root");
    }
    @After
    public void after() throws IOException {
        fs.close();
    }

    @Test
    public void listStatus() throws IOException, InterruptedException {
        FileStatus[] statuses = fs.listStatus(new Path("/"));
        for (FileStatus status:statuses){
            System.out.println(status);
        }
    }

    @Test
    public void delete() throws IOException, InterruptedException {
        boolean delete = fs.delete(new Path("/pom.xml"), true);
         delete = fs.delete(new Path("/hello.text"), true);
        System.out.println(delete);
    }
    @Test
    public void copyFromLocalFile() throws IOException, InterruptedException {
         fs.copyFromLocalFile(new Path("D:\\codes\\demo\\pom.xml"),new Path("/pom.xml"));//上传
    }

    @Test
    public void mkdirs() throws IOException, InterruptedException {
     fs.mkdirs(new Path("/zhifa"));
    }

    @Test
    public void copyToLocalFile() throws IOException, InterruptedException {//拉取文件
        fs.copyToLocalFile(new Path("/架构师资料.txt"),new Path("C:\\Users\\jack\\Desktop\\hadoop/架构师资料.txt"));
    }


}
