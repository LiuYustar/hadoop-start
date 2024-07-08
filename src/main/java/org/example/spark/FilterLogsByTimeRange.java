package org.example.spark;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class FilterLogsByTimeRange {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: FilterLogsByTimeRange <hdfsInputDirectory> <hdfsOutputDirectory>");
            System.exit(1);
        }

        String inputDirectory = args[0];
        String outputDirectory = args[1];

        // 设置Spark配置
        SparkConf conf = new SparkConf().setAppName("FilterLogsByTimeRange");

        // 创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建SparkSession
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // 读取HDFS目录下的所有.gz文件
        JavaRDD<String> logs = sc.textFile(inputDirectory + "/*.gz");

        // 时间范围
        String startTime = "14/Jun/2024:04:30:00";
        String endTime = "14/Jun/2024:04:30:59";

        // 解析日志并筛选出指定时间范围内的日志
        JavaRDD<String> filteredLogs = logs.filter(line -> {

            String timestampStr = line.split(" ")[4].replace("[", "");
            return timestampStr.compareTo(startTime) >= 0 && timestampStr.compareTo(endTime) <= 0;

        });

        // 保存筛选后的日志到HDFS并使用GZip压缩
        filteredLogs.saveAsTextFile(outputDirectory, GzipCodec.class);

        // 关闭SparkContext
        sc.close();
    }
}

