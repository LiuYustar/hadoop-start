package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.map.AccountMapper;

import java.io.IOException;

public class LogAccumulator {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 2) {
            System.err.println("Usage: LogAccumulator <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "hdfs://namenode:8020");
        Job job = Job.getInstance(conf, "LogAccumulator");
        job.setJarByClass(LogAccumulator.class);
        job.setMapperClass(AccountMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);

        if (success) {
            long value = job.getCounters().findCounter("CustomCounter", "TotalLines").getValue();
            System.out.println("Total number of lines: " + value);
        }

        System.exit(success ? 0 : 1);
    }
}
