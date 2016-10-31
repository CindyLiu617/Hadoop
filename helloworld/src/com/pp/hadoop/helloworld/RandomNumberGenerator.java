package com.pp.hadoop.helloworld;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Random;

final class RandomNumberGenerator {

    private RandomNumberGenerator() {
    }

    /**
     * Generate {@code count} random integers between {@code min} (inclusive)
     * and {@code max} {exclusive} and put into a HDFS file.
     */
    static void randomNumberGenerator(Configuration conf, int min, int max,
                                      Path outputPath, long count)
            throws IOException {
        if (max < min) {
            throw new IllegalArgumentException();
        }

        FileSystem hdfs = null;
        BufferedWriter br = null;
        try {
            hdfs = outputPath.getFileSystem(conf);
            Path file =
                    outputPath.getFileSystem(conf).makeQualified(outputPath);
            if (hdfs.exists(file)) {
                hdfs.delete(file, true);
            }
            OutputStream os = hdfs.create(file);
            br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));

            Random rand = new Random();
            while (count >= 0) {
                int writable = rand.nextInt(max - min) + min;
                br.write(Integer.toString(writable));
                br.newLine();
                --count;
            }
        } finally {
            if (br != null) {
                br.close();
            }
            if (hdfs != null) {
                hdfs.close();
            }
        }
    }
}
