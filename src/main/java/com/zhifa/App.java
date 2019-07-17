package com.zhifa;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException, InterruptedException {
        //System.setProperty("hadoop.home.dir", "D:\\hadoop-common-2.2.0-bin");
        System.setProperty("hadoop.home.dir", "C:\\Users\\jack\\Desktop\\hadoop\\hadoop\\src\\main\\resources");
        System.out.println( "Hello World!" );
        String uri = "hdfs://47.100.45.101:9000/";
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(URI.create(uri),conf,"root");
        //列出hdfs上 /input 目录下的所有文件
        FileStatus[] statuses = fs.listStatus(new Path("/"));
        for (FileStatus status:statuses){
            System.out.println(status);
        }


    }
}
