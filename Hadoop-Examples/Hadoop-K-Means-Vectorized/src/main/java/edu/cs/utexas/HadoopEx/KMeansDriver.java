package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeansDriver {

    public static void main(String[] args) throws Exception {

        if (args.length != 4) {
            System.err.println("Usage: KMeansDriver <input> <initialCentroids> <output> <maxIterations>");
            System.exit(1);
        }

        String inputPath = args[0];
        String centroidPath = args[1];
        String outputBasePath = args[2];
        int maxIterations = Integer.parseInt(args[3]);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        for (int i = 0; i < maxIterations; i++) {

            String iterationOutput = outputBasePath + "/iteration_" + i;

            // Delete output directory if exists
            Path outPath = new Path(iterationOutput);
            if (fs.exists(outPath)) {
                fs.delete(outPath, true);
            }

            Job job = Job.getInstance(conf, "KMeans Iteration " + i);
            job.setJarByClass(KMeansDriver.class);

            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            job.getConfiguration().set("centroid.path", centroidPath);

            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, outPath);

            boolean success = job.waitForCompletion(true);

            if (!success) {
                System.err.println("Iteration " + i + " failed.");
                System.exit(1);
            }

            // Update centroid path for next iteration
            centroidPath = iterationOutput + "/part-r-00000";
        }
    }
}
