package edu.cs.utexas.taxi;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;

// <Text, Text> => <Text, Float>

public class TaxiFilter extends Mapper<Text, Text, Text, FloatWritable> {

	PriorityQueue<SortableData> pq;
	int heapSize = 10;

    /**
     * Initializes the priorityQueue
     */
	public TaxiFilter() {
        pq = new PriorityQueue<>();
    }

    /**
     * Reads in results from the first job and filters the topk results
     *
     * @param key
     * @param value a float value stored as a string
     */
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    	
        float f = Float.parseFloat(value.toString());
        
        pq.add(new SortableData(new Text(key), new FloatWritable(f)));

        // keep the size of the priority queue <= heapSize
        if (pq.size() > heapSize) {
            pq.poll();
        }
    }

    /**
     * After the mapper is finished recieved new key, value pairs, flush the priority queue
     * Send the topK results to the reducer
     */
    public void cleanup(Context context) throws IOException, InterruptedException {
    	
        while (pq.size() > 0) {
            SortableData data = pq.poll();
            context.write(data.getKey(), data.getValue());
        }
        
    }
}


