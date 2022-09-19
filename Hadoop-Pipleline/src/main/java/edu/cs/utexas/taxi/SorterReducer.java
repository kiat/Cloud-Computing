
package edu.cs.utexas.taxi;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Collections;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Template reducer class for sorting the topK elements of a map reduce job
 */
public class SorterReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

	PriorityQueue<SortableData> pq;
    int heapSize = 10;

    /**
     * Initializes the priorityQueue
     */
    public SorterReducer() {
        pq = new PriorityQueue<>();
    }

    /**
     * Takes in the topK from each mapper and calculates the overall topK
     *
     * @param key
     * @param values size is 1 because key only has one distinct value
     */
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {

        // size of values is 1 because key only has one distinct value
        for (FloatWritable value : values) {
            pq.add(new SortableData(new Text(key), new FloatWritable(value.get())));
        }

        // keep the priorityQueue size <= heapSize
        while (pq.size() > heapSize) {
            pq.poll();
        }
    }

    /**
     * After all values have been received from the mappers, write out the overall topK
     */
    public void cleanup(Context context) throws IOException, InterruptedException {
        List<SortableData> values = new ArrayList<>();
        while (pq.size() > 0) {
            values.add(pq.poll());
        }

        // reverse so they are ordered in descending order
        Collections.reverse(values);

        for (SortableData data : values) {
            context.write(data.getKey(), data.getValue());
        }
    }
}
