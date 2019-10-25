package com.zhifa;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapDemo extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text k = new Text();
        IntWritable v = new IntWritable(1);
        String[] split = value.toString().split("[ ]+");
        for (String s : split) {
            k.set(s);
            context.write(k,v);
        }
    }
}
