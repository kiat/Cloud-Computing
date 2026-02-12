package edu.cs.utexas.HadoopEx;


import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class EMDriver {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        int K = 2;
        conf.setInt("K", K);

        // Initial parameters
        conf.set("means", "1.0,1.0;5.0,7.0");
        conf.set("vars", "1.0,1.0;1.0,1.0");
        conf.set("pis", "0.5,0.5");

        for (int iter = 0; iter < 5; iter++) {

            Job job = Job.getInstance(conf, "EM Iteration " + iter);
            job.setJarByClass(EMDriver.class);

            job.setMapperClass(EMMapper.class);
            job.setReducerClass(EMReducer.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            Path outPath = new Path(args[1] + "_iter" + iter);
            FileOutputFormat.setOutputPath(job, outPath);

            job.waitForCompletion(true);

            // Here you would:
            // 1. Read new parameters from output
            // 2. Update configuration for next iteration
        }
    }
}
