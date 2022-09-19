
package edu.cs.utexas.taxi;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;

/**
 * Stores the time driven and amount earned for one taxi ride
 */
public class TaxiWritable implements Writable {

    private final IntWritable seconds;
    private final FloatWritable money;

    

    /**
     * used by hadoop during de-serialization, initial values are overwritten
     * but necessary to avoid a NPE
     */
    public TaxiWritable(){
        this.seconds = new IntWritable(0);
        this.money = new FloatWritable(0);
    }

    /**
     * Constructs a DriverWritable object
     *
     * @param seconds number of seconds the ride lasted
     * @param money amount of money earned
     */
    public TaxiWritable(int seconds, float money) {
        this.seconds = new IntWritable(seconds);
        this.money = new FloatWritable(money);  
    }

    /**
     * @return the number of seconds the ride took
     */
    public int getSeconds() {
        return this.seconds.get();
    }

    /**
     * @return the amount of money earned
     */
    public float getMoney() {
        return this.money.get();
    }

    /**
     * Deserializes the driver writable object and sets the instance variables accordingly
     *
     * @param dataInput reads in the data
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        seconds.readFields(dataInput);
        money.readFields(dataInput);
    }

    /**
     * Serializes the driver writable object
     *
     * @param dataOutput where the instance variables are written
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        seconds.write(dataOutput);
        money.write(dataOutput);
    }    
}
