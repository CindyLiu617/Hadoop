package com.pp.hadoop.helloworld;

import static com.pp.hadoop.helloworld.RandomNumberGenerator.randomNumberGenerator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public final class MaxNum {

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        final Path input = new Path("/input/nums.txt");
        final Path outputDir = new Path("/output");

        // Generate inputs
        randomNumberGenerator(conf, 1, 100000000, input, 100000000);

        // Compress outputs
        conf.setBoolean("mapred.output.compress", true);
        conf.setClass("mapred.output.compression.codec", GzipCodec.class,
                      CompressionCodec.class);
        Job job = new Job(conf, "My Max Num");
        job.setJarByClass(MaxNum.class);
        job.setMapperClass(MaxNumMapper.class);
        job.setReducerClass(MaxNumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setCombinerClass(MaxNumCombiner.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, outputDir);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
