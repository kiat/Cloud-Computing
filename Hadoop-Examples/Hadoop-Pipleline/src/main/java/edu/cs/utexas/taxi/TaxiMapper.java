package edu.cs.utexas.taxi;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// <Object, Text> => <Text, DriverWritable>
public class TaxiMapper extends Mapper<Object, Text, Text, TaxiWritable> {

    /**
     * Maps a Taxi Ride line to a taxiId, <time of ride, amount earned> key value pair
     *
     * @param key not used
     * @param value line representing a taxi ride
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
    	String[] split = value.toString().split(",");

        // Check if data line has the correct number of attributes
        if (split.length != LineIds.NUMBER_IDS) {
            return;
        }

        try {
        	
            Text taxiId = new Text(split[LineIds.MEDALLION]);
            
            int duration = Integer.parseInt(split[LineIds.TRIP_TIME_SEC]);
            
            float money = Float.parseFloat(split[LineIds.TOTAL_AMOUNT]);

            // if duration is 0, erroneous data
            if (duration != 0) {
                context.write(taxiId, new TaxiWritable(duration, money));
            }
            
        } catch (Exception e) {
            // line is not valid, ignore
            e.printStackTrace();
        }
    }
}
