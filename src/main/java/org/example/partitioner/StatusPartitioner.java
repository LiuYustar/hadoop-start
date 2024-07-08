package org.example.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class StatusPartitioner extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
        if (numReduceTasks == 0) {
            return 0;
        }
        return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
