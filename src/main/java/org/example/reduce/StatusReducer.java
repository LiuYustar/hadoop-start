package org.example.reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPOutputStream;

public class StatusReducer extends Reducer<Text, Text, Text, Text> {

    private FileSystem fs;
    private Path outputPath;

    @Override
    protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        fs = FileSystem.get(conf);
        outputPath = FileOutputFormat.getOutputPath(context);
        if (!fs.exists(outputPath)) {
            fs.mkdirs(outputPath);
        }
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException {
        Path gzipOutpath = new Path(outputPath, key.toString() + ".gz");
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(fs.create(gzipOutpath));
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(gzipOutputStream, "UTF-8");

        for (Text value : values) {
            outputStreamWriter.write(value.toString());
            outputStreamWriter.write("\n");
        }

        outputStreamWriter.close();
        gzipOutputStream.close();
    }


}
