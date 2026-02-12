package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double sumX = 0;
        double sumY = 0;
        int count = 0;

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            sumX += Double.parseDouble(parts[0]);
            sumY += Double.parseDouble(parts[1]);
            count++;
        }

        double newX = sumX / count;
        double newY = sumY / count;

        context.write(key, new Text(newX + "," + newY));
    }
}
