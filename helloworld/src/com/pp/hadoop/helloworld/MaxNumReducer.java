package com.pp.hadoop.helloworld;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

class MaxNumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
                          Context context)
            throws IOException, InterruptedException {
        int temp = Integer.MIN_VALUE;
        for (IntWritable value : values) {
            if (value.get() > temp) {
                temp = value.get();
            }
        }
        context.write(new Text(), new IntWritable(temp));
    }

}
