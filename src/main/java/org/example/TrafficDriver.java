package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.map.TrafficMapper;
import org.example.reduce.TrafficReducer;

import java.io.IOException;

public class TrafficDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 2) {
            System.out.println("Usage: TrafficDriver <input path> <output path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        conf.set("fs.defaultFs", "hdfs://hadoop001:8020");
        Job job = Job.getInstance(conf, "Traffic Analysis");
        job.setJarByClass(TrafficDriver.class);
        job.setMapperClass(TrafficMapper.class);
        job.setReducerClass(TrafficReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
