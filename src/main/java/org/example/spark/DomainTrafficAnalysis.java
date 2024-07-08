package org.example.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

public class DomainTrafficAnalysis {

    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
    private static final DateTimeFormatter minuteFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: DomainTrafficAnalysis <hdfsDirectory>");
            System.exit(1);
        }

        String inputDirectory = args[0];

        // 设置Spark配置
        SparkConf conf = new SparkConf().setAppName("DomainTrafficAnalysis");

        // 创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建SparkSession
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // 读取HDFS目录下的所有.gz文件
        JavaRDD<String> logs = sc.textFile(inputDirectory + "/*.gz");

        // 解析日志并提取域名、流量和时间字段
        JavaPairRDD<String, Long> domainTraffic = logs.mapToPair(line -> {
            String[] parts = line.split(" ");
            String domain = parts[3];
            String timeString = parts[4].replace("[", "");
            long traffic = Long.parseLong(parts[11]);
            LocalDateTime timestamp = LocalDateTime.parse(timeString, dateTimeFormatter);
            String minuteKey = getMinuteKey(timestamp);
            return new Tuple2<>(domain + "_" + minuteKey, traffic);
        });

        // 按分钟聚合流量
        JavaPairRDD<String, Long> aggregatedTraffic = domainTraffic.reduceByKey(Long::sum);

        // 保存到HBase
        aggregatedTraffic.foreachPartition(iterator -> {
            Configuration hbaseConfig = HBaseConfiguration.create();
            hbaseConfig.set("hbase.zookeeper.quorum", "myhbase");
            hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181");
            try (Connection connection = ConnectionFactory.createConnection(hbaseConfig);
                 Table table = connection.getTable(TableName.valueOf("spark_traffic"))) {

                while (iterator.hasNext()) {
                    Tuple2<String, Long> record = iterator.next();
                    String[] keyParts = record._1.split("_");
                    String domain = keyParts[0];
                    String minuteKey = keyParts[1];

                    Put put = new Put(Bytes.toBytes(domain));
                    put.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes(minuteKey), Bytes.toBytes(record._2));

                    table.put(put);
                }
            }
        });

        // 关闭SparkContext
        sc.close();
    }

    private static String getMinuteKey(LocalDateTime timestamp) {
        LocalDateTime truncated = timestamp.truncatedTo(ChronoUnit.MINUTES);
        return truncated.format(minuteFormatter);
    }
}

