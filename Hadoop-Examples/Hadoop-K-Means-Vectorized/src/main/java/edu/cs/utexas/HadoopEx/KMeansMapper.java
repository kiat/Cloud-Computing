package edu.cs.utexas.HadoopEx;

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private List<double[]> centroids = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        String centroidPath = conf.get("centroid.path");

        Path path = new Path(centroidPath);
        FileSystem fs = FileSystem.get(conf);

        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));

        String line;
        while ((line = br.readLine()) != null) {
            centroids.add(parseVector(line.trim()));
        }
        br.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        double[] point = parseVector(value.toString().trim());
        int nearestCentroid = getNearestCentroid(point);

        context.write(new IntWritable(nearestCentroid), value);
    }

    private int getNearestCentroid(double[] point) {
        double minDistance = Double.MAX_VALUE;
        int index = 0;

        for (int i = 0; i < centroids.size(); i++) {
            double distance = euclideanDistance(point, centroids.get(i));
            if (distance < minDistance) {
                minDistance = distance;
                index = i;
            }
        }
        return index;
    }

    private double euclideanDistance(double[] a, double[] b) {
        double sum = 0;
        for (int i = 0; i < a.length; i++) {
            sum += Math.pow(a[i] - b[i], 2);
        }
        return Math.sqrt(sum);
    }

    private double[] parseVector(String line) {
        String[] tokens = line.split(",");
        double[] vector = new double[tokens.length];

        for (int i = 0; i < tokens.length; i++) {
            vector[i] = Double.parseDouble(tokens[i]);
        }

        return vector;
    }
}
