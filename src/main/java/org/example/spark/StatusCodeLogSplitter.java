package org.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StatusCodeLogSplitter {
    public static void main(String[] args) {
        // 配置Spark
        SparkConf conf = new SparkConf().setAppName("StatusCodeLogSplitter").setMaster("yarn");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // HDFS中的.gz文件路径
        String inputPath = args[0];
        String outputPath = args[1];

        // 读取.gz文件
        JavaRDD<String> lines = sc.textFile(inputPath + "/*.gz");

        // 提取status字段并分组
        Map<String, List<String>> statusGroupedLines = lines.mapToPair((PairFunction<String, String, String>) line -> {
            String status = extractStatus(line); // 提取status字段的自定义方法
            return new Tuple2<>(status, line);
        }).groupByKey().mapToPair(tuple -> new Tuple2<>(tuple._1, (Iterable<String>) tuple._2))
                .collectAsMap()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                    List<String> list = new ArrayList<>();
                    entry.getValue().forEach(list::add);
                    return list;
                }));

        // 保存分组后的数据到HDFS中的.gz文件
        statusGroupedLines.forEach((status, linesList) -> {
            JavaRDD<String> rdd = sc.parallelize(linesList);
            rdd.saveAsTextFile(outputPath + "/status_" + status + ".gz");
        });

        sc.close();
    }

    private static String extractStatus(String line) {
        // 假设log格式类似于：timestamp domain status traffic ...
        String[] fields = line.split(" ");
        return fields[10]; // 返回status字段
    }
}
