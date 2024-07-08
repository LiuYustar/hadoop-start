package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.example.map.StatusMapper;
import org.example.reduce.StatusReducer;

import java.io.IOException;

public class StatusDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 2) {
            System.err.println("Usage: StatusDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "hdfs://hadoop001:8020");
        Job job = Job.getInstance(conf, "statusCode Partition");
        job.setJarByClass(StatusDriver.class);
        job.setMapperClass(StatusMapper.class);
        job.setReducerClass(StatusReducer.class);
//        job.setPartitionerClass(StatusPartitioner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

//        job.setNumReduceTasks(5);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
