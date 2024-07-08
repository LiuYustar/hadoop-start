package org.example.reduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopURLReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private TreeMap<Long, String> top10Map = new TreeMap<>();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable value : values) {
            sum += value.get();
        }

        // 将URL和对应的流量放入TreeMap，按流量排序
        top10Map.put(sum, key.toString());

        // 如果超过10个URL，则移除流量最小的那个
        if (top10Map.size() > 10) {
            top10Map.remove(top10Map.firstKey());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 输出Top 10 URL及其流量
        for (Map.Entry<Long, String> entry : top10Map.descendingMap().entrySet()) {
            context.write(new Text(entry.getValue()), new LongWritable(entry.getKey()));
        }
    }
}

