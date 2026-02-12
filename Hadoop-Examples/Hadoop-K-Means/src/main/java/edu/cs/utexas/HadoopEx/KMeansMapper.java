package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private List<double[]> centroids = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException {
        // Read centroids from configuration
        String centroidStr = context.getConfiguration().get("centroids");
        String[] centroidTokens = centroidStr.split(";");

        for (String token : centroidTokens) {
            String[] parts = token.split(",");
            double x = Double.parseDouble(parts[0]);
            double y = Double.parseDouble(parts[1]);
            centroids.add(new double[]{x, y});
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] parts = value.toString().split(",");
        double x = Double.parseDouble(parts[0]);
        double y = Double.parseDouble(parts[1]);

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
}
