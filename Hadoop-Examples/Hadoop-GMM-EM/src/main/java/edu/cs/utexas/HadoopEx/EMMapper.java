package edu.cs.utexas.HadoopEx;


import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class EMMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private int K;
    private double[][] means;
    private double[][] variances;
    private double[] pis;

    @Override
    protected void setup(Context context) {

        K = context.getConfiguration().getInt("K", 2);

        means = parseMatrix(context.getConfiguration().get("means"), K, 2);
        variances = parseMatrix(context.getConfiguration().get("vars"), K, 2);
        pis = parseArray(context.getConfiguration().get("pis"), K);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] parts = value.toString().split(",");
        double x1 = Double.parseDouble(parts[0]);
        double x2 = Double.parseDouble(parts[1]);

        double[] responsibilities = new double[K];
        double total = 0.0;

        for (int k = 0; k < K; k++) {
            double prob = gaussian(x1, x2, means[k], variances[k]);
            responsibilities[k] = pis[k] * prob;
            total += responsibilities[k];
        }

        for (int k = 0; k < K; k++) {
            double gamma = responsibilities[k] / total;

            // Emit:
            // key = cluster
            // value = gamma, gamma*x1, gamma*x2, gamma*x1^2, gamma*x2^2
            String out = gamma + "," +
                    (gamma * x1) + "," +
                    (gamma * x2) + "," +
                    (gamma * x1 * x1) + "," +
                    (gamma * x2 * x2);

            context.write(new IntWritable(k), new Text(out));
        }
    }

    private double gaussian(double x1, double x2, double[] mean, double[] var) {
        double dx1 = x1 - mean[0];
        double dx2 = x2 - mean[1];

        double exp = -0.5 * ((dx1 * dx1 / var[0]) + (dx2 * dx2 / var[1]));
        double denom = 2 * Math.PI * Math.sqrt(var[0] * var[1]);

        return Math.exp(exp) / denom;
    }

    private double[][] parseMatrix(String str, int rows, int cols) {
        double[][] matrix = new double[rows][cols];
        String[] tokens = str.split(";");
        for (int i = 0; i < rows; i++) {
            String[] parts = tokens[i].split(",");
            for (int j = 0; j < cols; j++)
                matrix[i][j] = Double.parseDouble(parts[j]);
        }
        return matrix;
    }

    private double[] parseArray(String str, int size) {
        double[] arr = new double[size];
        String[] parts = str.split(",");
        for (int i = 0; i < size; i++)
            arr[i] = Double.parseDouble(parts[i]);
        return arr;
    }
}
