package org.example.reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TrafficReducer extends Reducer<Text, LongWritable, Text, Text> {
    private static final String TABLE_NAME = "traffic_table";
    private static final String CF_NAME = "cf1";

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long totalTraffic = 0;
        for (LongWritable value : values) {
            totalTraffic += value.get();
        }

        try {
            // 初始化HBase连接和表
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "myhbase");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            Connection conn = ConnectionFactory.createConnection(conf);
            Table table = conn.getTable(TableName.valueOf(TABLE_NAME));

            // 将域名和时间拆分出来
            String[] parts = key.toString().split(" ");
            String domain = parts[0];
            String minuteKey = parts[1];

            // 构造HBase的Put对象
            Put put = new Put(Bytes.toBytes(domain));
            put.addColumn(Bytes.toBytes(CF_NAME), Bytes.toBytes(minuteKey), Bytes.toBytes(totalTraffic));

            // 将数据写入HBase
            table.put(put);

            // 关闭连接和表
            table.close();
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
