package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.map.LogFilterMapper;
import org.example.reduce.LogFilterReducer;

public class LogFilterDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: LogFilterDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "hdfs://hadoop001:8020");
        Job job = Job.getInstance(conf, "Log Filter");
        job.setJarByClass(LogFilterDriver.class);
        job.setMapperClass(LogFilterMapper.class);
        job.setReducerClass(LogFilterReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

