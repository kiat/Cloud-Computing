# Create a JAR Using Maven 

To compile the project and create a single jar file with all dependencies: 
	
```mvn clean package```


## How to run 

```
java -jar target/MapReduce-Partitioner-example-0.1-SNAPSHOT-jar-with-dependencies.jar Book-Tiny.txt  output
```

# Hadoop MapReduce Partitioner 

## Important Concept

* Partitioner controls which reducer gets which key
* Number of reducers controls: Parallelism + number of output files
* Formula always: partition_id = getPartition(key) % numReduceTasks


| Num Reducers | Result                                  |
| ------------ | --------------------------------------- |
| 1            | All keys in one output                  |
| 2            | Keys distributed across 2 reducers      |
| 3            | Each alphabet range in separate reducer |

Change the number of Reducers to different numbers to generate these different outputs. 

##  HashPartitioner
```Java 
public class HashPartitioner<K, V>
extends Partitioner<K, V> {

    public int getPartition(K key, V value,
                            int numReduceTasks) {
        return (key.hashCode() & Integer.MAX_VALUE)
               % numReduceTasks;
    }
}
```
It does: 
* Even distribution (usually)
* No semantic grouping
* Good for general use

## Length-Based Partitioner


Group words by their length.

**Example:**
* Reducer 0 → short words (<=3)
* Reducer 1 → medium words (4–6)
* Reducer 2 → long words (>6)

```Java
public static class LengthPartitioner
        extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text key, IntWritable value,
                            int numReduceTasks) {

        int length = key.toString().length();

        if (length <= 3)
            return 0 % numReduceTasks;
        else if (length <= 6)
            return 1 % numReduceTasks;
        else
            return 2 % numReduceTasks;
    }
}
```

# Frequency-Aware Partitioner (Skew Handling)
If one word appears millions of times (e.g., "the"), it causes skew.

You can:

* Detect heavy keys
* Route them to dedicated reducers

```Java
public static class SkewAwarePartitioner
        extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text key, IntWritable value,
                            int numReduceTasks) {

        if (key.toString().equals("the"))
            return 0;

        return (key.hashCode() & Integer.MAX_VALUE)
                % numReduceTasks;
    }
}
```

Use case: 
* Avoid reducer bottlenecks
* Handle hot keys



# Random Partitioner

```Java 
import java.util.Random;

public static class RandomPartitioner
        extends Partitioner<Text, IntWritable> {

    private Random rand = new Random();

    @Override
    public int getPartition(Text key, IntWritable value,
                            int numReduceTasks) {

        return rand.nextInt(numReduceTasks);
    }
}
```

**Warning**

This breaks correctness for WordCount
Same key may go to multiple reducers.

Use only when:

* Keys are independent
* You don't require global aggregation


# Range Partitioner (Ordered Output)

Useful when:

* You want globally sorted output
* You know key distribution

Example: Numeric keys

```Java
public static class RangePartitioner
        extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text key, IntWritable value,
                            int numReduceTasks) {

        int num = Integer.parseInt(key.toString());

        if (num < 1000)
            return 0;
        else if (num < 5000)
            return 1;
        else
            return 2;
    }
}
```

Use cases: 
* Total order sorting
* Range queries
* Secondary sorting


# First-Character Hash Bucketing

Instead of fixed ranges (A–I), dynamically bucket:

```Java 
public static class AlphabetHashPartitioner
        extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text key, IntWritable value,
                            int numReduceTasks) {

        char first = key.toString()
                        .toLowerCase()
                        .charAt(0);

        return (first - 'a') % numReduceTasks;
    }
}
```
More flexible than hard-coded ranges.

# Multi-Field Partitioner

If your key is composite:

```userId,productId```

```Java
String[] parts = key.toString().split(",");
String userId = parts[0];

return (userId.hashCode() & Integer.MAX_VALUE)
        % numReduceTasks;
```

Very common in:

* Join jobs
* Secondary sorting
* Log processing



# General Notes on Partitioner

In real distributed systems:

* Default hash partitioning is enough 80% of the time.

Custom partitioners are critical when:
* Data skew exists
* You need sorted output
* You're doing joins
* You're optimizing reducer load








