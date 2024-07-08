package org.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;

public class Top10URLsByTraffic {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: Top10URLsByTraffic <hdfsInputDirectory>");
            System.exit(1);
        }

        String inputDirectory = args[0];
        String outputDirectory = args[1];

        // 设置Spark配置
        SparkConf conf = new SparkConf().setAppName("Top10URLsByTraffic");

        // 创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建SparkSession
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // 读取HDFS目录下的所有.gz文件
        JavaRDD<String> logs = sc.textFile(inputDirectory + "/*.gz");

        // 解析日志并提取URL和流量字段
        JavaPairRDD<String, Long> urlTraffic = logs.mapToPair(line -> {
            String[] parts = line.split(" ");
            String url = parts[8];
            long traffic = Long.parseLong(parts[11]);
            return new Tuple2<>(url, traffic);

        }).filter(tuple -> !tuple._1.isEmpty());

        // 按URL聚合流量
        JavaPairRDD<String, Long> aggregatedTraffic = urlTraffic.reduceByKey(Long::sum);

        // 按流量排序并取Top 10
        List<Tuple2<Long, String>> top10URLs = aggregatedTraffic
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .take(10);

        // 转换为RDD
        JavaRDD<String> top10URLsRDD = sc.parallelize(top10URLs).map(tuple -> "URL: " + tuple._2 + ", Traffic: " + tuple._1);
        // 保存到HDFS
        top10URLsRDD.saveAsTextFile(outputDirectory);

        // 关闭SparkContext
        sc.close();
    }
}

