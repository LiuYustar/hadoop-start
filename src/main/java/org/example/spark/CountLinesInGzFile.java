package org.example.spark;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class CountLinesInGzFile {

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: CountLinesInGzFile <hdfsFilePath>");
            System.exit(1);
        }
        String hdfsPath = args[0];
        String outputFilePath = args[1];
        // 设置Spark配置
        SparkConf conf = new SparkConf().setAppName("CountLinesInGzFile");

        // 创建SparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建SparkSession
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        // 读取.gz文件
        long numLines = sc.textFile(hdfsPath + "/*.gz").count();

        // 将行数保存到HDFS
        FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
        Path outputPath = new Path(outputFilePath);
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(outputPath)))) {
            writer.write("Number of lines in the file: " + numLines);
            writer.write("\n");
        }

        // 关闭SparkContext
        sc.close();
    }
}

