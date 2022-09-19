
package edu.cs.utexas.taxi;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TaxiReducer extends Reducer<Text, TaxiWritable, Text, FloatWritable> {

    /**
     * Calculates the amount of money earned per minute for each distinct Taxi
     *
     * @param taxiID id of the Taxi 
     * @param values list of rides of the driver including time and amount earned for each ride
     */
    public void reduce(Text taxiID, Iterable<TaxiWritable> values, Context context)
            throws IOException, InterruptedException {

        int totalSeconds = 0;
        float totalMoney = 0;

        // calculate total seconds driven and total amount earned
        for (TaxiWritable value : values) {
            totalSeconds += value.getSeconds();
            totalMoney += value.getMoney();
        }

        float moneyPerMinute = totalMoney / ((float) totalSeconds / 60);

        context.write(taxiID, new FloatWritable(moneyPerMinute));
    }
}