package edu.cs.utexas.HadoopEx;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double[] sum = null;
        int count = 0;

        for (Text value : values) {
            String[] tokens = value.toString().split(",");

            if (sum == null) {
                sum = new double[tokens.length];
            }

            for (int i = 0; i < tokens.length; i++) {
                sum[i] += Double.parseDouble(tokens[i]);
            }

            count++;
        }

        if (count == 0) return;

        for (int i = 0; i < sum.length; i++) {
            sum[i] /= count;
        }

        StringBuilder newCentroid = new StringBuilder();
        for (int i = 0; i < sum.length; i++) {
            newCentroid.append(sum[i]);
            if (i < sum.length - 1) {
                newCentroid.append(",");
            }
        }

        // Write ONLY centroid vector (no key, no tab)
        context.write(NullWritable.get(), new Text(newCentroid.toString()));
    }
}
