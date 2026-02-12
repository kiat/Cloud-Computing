package edu.cs.utexas.HadoopEx;


import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class EMReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double sumGamma = 0;
        double sumX1 = 0, sumX2 = 0;
        double sumX1Sq = 0, sumX2Sq = 0;

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            double gamma = Double.parseDouble(parts[0]);

            sumGamma += gamma;
            sumX1 += Double.parseDouble(parts[1]);
            sumX2 += Double.parseDouble(parts[2]);
            sumX1Sq += Double.parseDouble(parts[3]);
            sumX2Sq += Double.parseDouble(parts[4]);
        }

        double mu1 = sumX1 / sumGamma;
        double mu2 = sumX2 / sumGamma;

        double var1 = (sumX1Sq / sumGamma) - mu1 * mu1;
        double var2 = (sumX2Sq / sumGamma) - mu2 * mu2;

        String output = mu1 + "," + mu2 + ";" +
                var1 + "," + var2 + ";" +
                sumGamma;

        context.write(key, new Text(output));
    }
}
