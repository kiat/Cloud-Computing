package edu.cs.utexas.HadoopEx;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

// Custom Partitioner
public class AlphabetPartitioner extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {

        if (numReduceTasks == 0) {
            return 0;
        }

        char firstChar = key.toString().toLowerCase().charAt(0);

        if (firstChar >= 'a' && firstChar <= 'i') {
            return 0 % numReduceTasks;
        } else if (firstChar >= 'j' && firstChar <= 'r') {
            return 1 % numReduceTasks;
        } else {
            return 2 % numReduceTasks;
        }
    }
}