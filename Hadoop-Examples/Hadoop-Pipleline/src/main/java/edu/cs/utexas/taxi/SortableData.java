package edu.cs.utexas.taxi;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

/**
 * Class used by the filter and sorter classes to rank data
 */
public class SortableData implements Comparable<SortableData> {
    private final Text key;
    private final FloatWritable value;

    /**
     * Creates a SortData object
     */
    public SortableData(Text key, FloatWritable value) {
        this.key = key;
        this.value = value;
    }

    /**
     * @return the key of the SortData object
     */
    public Text getKey() {
        return key;
    }

    /**
     * @return the value of the SortData object
     */
    public FloatWritable getValue() {
        return value;
    }

    /**
     * Compares two sort data objects by their value.
     * @param other
     * @return 0 if equal, negative if this < other, positive if this > other
     */
    @Override
    public int compareTo(SortableData other) {
        float diff = value.get() - other.value.get();
        if (diff > 0) {
            return 1;
        } else if (diff < 0) {
            return -1;
        }

        return 0;
    }
}
