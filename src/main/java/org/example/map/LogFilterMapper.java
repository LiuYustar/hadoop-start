package org.example.map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class LogFilterMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
    private static final String TARGET_DATE = "14/Jun/2024";
    private static final LocalDateTime START_TIME = LocalDateTime.parse(TARGET_DATE + ":04:30:00", dateTimeFormatter);
    private static final LocalDateTime END_TIME = LocalDateTime.parse(TARGET_DATE + ":04:30:59", dateTimeFormatter);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] fields = line.split(" ");
        String timeString = fields[4].replace("[", "");
        try {
            LocalDateTime timestamp = LocalDateTime.parse(timeString, dateTimeFormatter);
            if (!timestamp.isBefore(START_TIME) && !timestamp.isAfter(END_TIME)) {
                context.write(key, value);
            }
        } catch (Exception e) {
            // 忽略解析错误的日志记录
            e.printStackTrace();
        }

    }
}

