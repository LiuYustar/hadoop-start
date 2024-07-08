package org.example.map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TopURLMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] fields = line.split(" ");
        String url = fields[8];
        long traffic = Long.parseLong(fields[11]);
        context.write(new Text(url), new LongWritable(traffic));
    }
}

