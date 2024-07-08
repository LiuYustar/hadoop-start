package org.example.map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

public class TrafficMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
    private static final DateTimeFormatter minuteFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] fields = line.split(" ");
        String timeString = fields[4].replace("[", "");
        String domain = fields[3];
        long traffic = Long.parseLong(fields[11]);
        LocalDateTime timestamp = LocalDateTime.parse(timeString, dateTimeFormatter);
        String minuteKey = getMinuteKey(timestamp);
        context.write(new Text(domain + " " + minuteKey), new LongWritable(traffic));
    }

    private String getMinuteKey(LocalDateTime timestamp) {
        LocalDateTime truncated = timestamp.truncatedTo(ChronoUnit.MINUTES);
        return truncated.format(minuteFormatter);
    }
}
