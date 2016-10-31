package com.pp.hadoop.helloworld;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

class MaxNumMapper extends Mapper<Object, Text, Text, IntWritable> {

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        context.write(new Text(),
                      new IntWritable(Integer.parseInt(value.toString())));
    }

}
