
package edu.cs.utexas.taxi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TaxiHadoopDriver extends Configured implements Tool {

	/**
	 * Main function that runs task 3
	 *
	 * @param args [<input file>, <temp intermediate folder>, <output folder>]
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TaxiHadoopDriver(), args);
		System.exit(res);
	}

	/**
	 * Connects the first job with the second
	 */
	public int run(String[] args) {
		try {
			Configuration conf = new Configuration();

			// Run first job: compute money per minute for each driver

			Job job = new Job(conf, "Driver");
			job.setJarByClass(TaxiHadoopDriver.class);

			// specify a Mapper
			job.setMapperClass(TaxiMapper.class);
			
			// specify a Reducer
			job.setReducerClass(TaxiReducer.class);
			
			// specify output types
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(TaxiWritable.class);

			// specify input and output directories
			FileInputFormat.addInputPath(job, new Path(args[0]));
			job.setInputFormatClass(TextInputFormat.class);

			// This is a place to store intermediate data
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.setOutputFormatClass(TextOutputFormat.class);

			if (!job.waitForCompletion(true)) {
				return 1;
			}

			// run second job: compute the top 10 Taxi 	
			Configuration conf2 = new Configuration();

			Job job2 = new Job(conf2, "Taxi Topk");
			job2.setJarByClass(TaxiHadoopDriver.class);
			job2.setNumReduceTasks(1);
			
			// specify a Mapper
			job2.setMapperClass(TaxiFilter.class);
			
			// specify a Reducer
			job2.setReducerClass(SorterReducer.class);

			// specify output types
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(FloatWritable.class);

			// specify input and output directories
			// We read the intermediate data
			FileInputFormat.addInputPath(job2, new Path(args[1]));
			job2.setInputFormatClass(KeyValueTextInputFormat.class);

			FileOutputFormat.setOutputPath(job2, new Path(args[2]));
			job2.setOutputFormatClass(TextOutputFormat.class);

			return (job2.waitForCompletion(true) ? 0 : 1);
			
		} catch (InterruptedException | ClassNotFoundException | IOException e) {
			System.err.println("Error during driver job.");
			e.printStackTrace();
			return 2;
		}
	}
}

