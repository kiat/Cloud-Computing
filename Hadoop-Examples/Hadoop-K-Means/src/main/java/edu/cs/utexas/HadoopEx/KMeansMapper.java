package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private enum ParseCounters {
        EMPTY_INPUT_LINE,
        MALFORMED_CENTROID,
        MALFORMED_POINT
    }

    private List<double[]> centroids = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException {
        // Read centroids from configuration
        String centroidStr = context.getConfiguration().get("centroids");
        if (centroidStr == null || centroidStr.trim().isEmpty()) {
            throw new IOException("Missing 'centroids' configuration");
        }

        String[] centroidTokens = centroidStr.split(";");

        for (String token : centroidTokens) {
            double[] parsed = parsePoint(token);
            if (parsed == null) {
                context.getCounter(ParseCounters.MALFORMED_CENTROID).increment(1);
                continue;
            }
            centroids.add(parsed);
        }

        if (centroids.isEmpty()) {
            throw new IOException("No valid centroids found in configuration");
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty()) {
            context.getCounter(ParseCounters.EMPTY_INPUT_LINE).increment(1);
            return;
        }

        double[] point = parsePoint(line);
        if (point == null) {
            context.getCounter(ParseCounters.MALFORMED_POINT).increment(1);
            return;
        }

        double x = point[0];
        double y = point[1];

        double minDist = Double.MAX_VALUE;
        int closestCentroid = 0;

        for (int i = 0; i < centroids.size(); i++) {
            double dx = x - centroids.get(i)[0];
            double dy = y - centroids.get(i)[1];
            double distance = Math.sqrt(dx * dx + dy * dy);

            if (distance < minDist) {
                minDist = distance;
                closestCentroid = i;
            }
        }

        context.write(new IntWritable(closestCentroid), value);
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
