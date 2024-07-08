package org.example.map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StatusMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text statusCode = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] parts = line.split(" ");
        String status = parts[10];
        statusCode.set(status);
        context.write(statusCode, value);
    }
}
