package edu.cs.utexas.HadoopEx;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.*;
import java.util.*;

public class KMeansDriver {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        String inputPath = args[0];
        String outputPath = args[1];

        // Initial centroids
        String centroids = "1.0,1.0;5.0,7.0";

        int maxIterations = 5;

        for (int i = 0; i < maxIterations; i++) {

            conf.set("centroids", centroids);

            Job job = Job.getInstance(conf, "KMeans Iteration " + i);
            job.setJarByClass(KMeansDriver.class);

            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(inputPath));
            Path outPath = new Path(outputPath + "_iter" + i);
            FileOutputFormat.setOutputPath(job, outPath);

            job.waitForCompletion(true);

            // Read new centroids from output
            centroids = readCentroids(outPath, conf);
        }
    }

    private static String readCentroids(Path path, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path file = new Path(path, "part-r-00000");
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file)));

        String line;
        StringBuilder centroidStr = new StringBuilder();

        while ((line = br.readLine()) != null) {
            String[] parts = line.split("\t");
            centroidStr.append(parts[1]).append(";");
        }

        br.close();
        return centroidStr.toString().replaceAll(";$", "");
    }
}
