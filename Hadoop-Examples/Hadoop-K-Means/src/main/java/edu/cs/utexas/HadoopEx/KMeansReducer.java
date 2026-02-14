package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    private enum ParseCounters {
        MALFORMED_REDUCER_VALUE,
        EMPTY_REDUCER_CLUSTER
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double sumX = 0;
        double sumY = 0;
        int count = 0;

        for (Text val : values) {
            double[] point = parsePoint(val.toString());
            if (point == null) {
                context.getCounter(ParseCounters.MALFORMED_REDUCER_VALUE).increment(1);
                continue;
            }

            sumX += point[0];
            sumY += point[1];
            count++;
        }

        if (count == 0) {
            context.getCounter(ParseCounters.EMPTY_REDUCER_CLUSTER).increment(1);
            return;
        }

        double newX = sumX / count;
        double newY = sumY / count;

        context.write(key, new Text(newX + "," + newY));
    }

    private double[] parsePoint(String raw) {
        String[] parts = raw.split(",");
        if (parts.length != 2) {
            return null;
        }
        try {
            double x = Double.parseDouble(parts[0].trim());
            double y = Double.parseDouble(parts[1].trim());
            return new double[]{x, y};
        } catch (NumberFormatException ex) {
            return null;
        }
    }
}
