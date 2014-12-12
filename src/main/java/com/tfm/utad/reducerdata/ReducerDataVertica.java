/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tfm.utad.reducerdata;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReducerDataVertica {

    private static final String HDFS_LOCALHOST_LOCALDOMAIN = "hdfs://172.16.134.128/";
    private static final String FS_DEFAULT_FS = "fs.defaultFS";

    private final static Logger LOG = LoggerFactory.getLogger(ReducerDataVertica.class);

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {

        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd-HH-mm-ss");
        Date date = new Date();

        Path inputPath = new Path("/home/jab/camus/reducer-data-vertica");
        Path outputDir = new Path("/home/jab/camus/verticadb/" + sdf.format(date));

        // Create configuration
        Configuration conf = new Configuration(true);
        conf.set(FS_DEFAULT_FS, HDFS_LOCALHOST_LOCALDOMAIN);
        FileSystem fs = FileSystem.get(conf);
        Path filesPath = new Path(inputPath + "/*");
        FileStatus[] files = fs.globStatus(filesPath);

        // Create job
        Job job = new Job(conf, "ReducerDataVertica");
        job.setJarByClass(ReducerDataVertica.class);

        // Setup MapReduce
        job.setMapperClass(ReducerDataVerticaMapper.class);
        job.setReducerClass(ReducerDataVerticaReducer.class);
        job.setNumReduceTasks(1);

        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ReducerVerticaValue.class);

        // Input
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Delete output if exists
        if (fs.exists(outputDir)) {
            fs.delete(outputDir, true);
        }

        // Execute job
        int code = job.waitForCompletion(true) ? 0 : 1;
        if (code == 0) {
            Counters counters = job.getCounters();
            Counter malformedCounter = counters.findCounter(ReducerDataEnum.MALFORMED_DATA);
            LOG.info("Counter malformed data: " + malformedCounter.getValue());
            for (FileStatus fStatus : files) {
                LOG.info("File name:" + fStatus.getPath());
                if (fStatus.isFile()) {
                    LOG.info("Removing file in path:" + fStatus.getPath());
                    fs.delete(fStatus.getPath(), false);
                }
            }
        }
    }
}