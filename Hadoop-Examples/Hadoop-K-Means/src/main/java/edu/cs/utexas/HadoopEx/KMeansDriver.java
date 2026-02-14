package edu.cs.utexas.HadoopEx;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KMeansDriver {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: KMeansDriver <inputPath> <outputBasePath>");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        String inputPath = args[0];
        String outputPath = args[1];

        // Initial centroids
        String centroids = "1.0,1.0;5.0,7.0";

        int maxIterations = 5;

        for (int i = 0; i < maxIterations; i++) {

            conf.set("centroids", centroids);

            Job job = Job.getInstance(conf, "KMeans Iteration " + i);
            job.setJarByClass(KMeansDriver.class);

            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(inputPath));
            Path outPath = new Path(outputPath + "_iter" + i);
            FileOutputFormat.setOutputPath(job, outPath);

            boolean ok = job.waitForCompletion(true);
            if (!ok) {
                throw new IOException("KMeans iteration " + i + " failed");
            }

            // Read new centroids from output
            centroids = readCentroids(outPath, conf, centroids);
        }
    }

    private static String readCentroids(Path path, Configuration conf, String previousCentroids)
            throws IOException {
        FileSystem fs = FileSystem.get(conf);
        List<double[]> oldCentroids = parseCentroidList(previousCentroids);
        List<double[]> mergedCentroids = cloneCentroids(oldCentroids);

        FileStatus[] outputParts = fs.globStatus(new Path(path, "part-r-*"));
        if (outputParts == null || outputParts.length == 0) {
            throw new IOException("No reducer output files found under " + path);
        }

        Pattern tabPattern = Pattern.compile("^(\\d+)\\t(.+)$");
        for (FileStatus part : outputParts) {
            try (BufferedReader br =
                         new BufferedReader(new InputStreamReader(fs.open(part.getPath())))) {
                String line;
                while ((line = br.readLine()) != null) {
                    Matcher matcher = tabPattern.matcher(line.trim());
                    if (!matcher.matches()) {
                        continue;
                    }

                    int idx;
                    try {
                        idx = Integer.parseInt(matcher.group(1));
                    } catch (NumberFormatException ex) {
                        continue;
                    }

                    if (idx < 0 || idx >= mergedCentroids.size()) {
                        continue;
                    }

                    double[] parsed = parsePoint(matcher.group(2));
                    if (parsed == null) {
                        continue;
                    }
                    mergedCentroids.set(idx, parsed);
                }
            }
        }

        return stringifyCentroids(mergedCentroids);
    }

    private static List<double[]> parseCentroidList(String centroidText) throws IOException {
        List<double[]> result = new ArrayList<>();
        if (centroidText == null || centroidText.trim().isEmpty()) {
            throw new IOException("Centroid string is empty");
        }

        String[] tokens = centroidText.split(";");
        for (String token : tokens) {
            double[] parsed = parsePoint(token);
            if (parsed == null) {
                throw new IOException("Invalid centroid value: " + token);
            }
            result.add(parsed);
        }

        if (result.isEmpty()) {
            throw new IOException("No valid centroids parsed from: " + centroidText);
        }

        return result;
    }

    private static List<double[]> cloneCentroids(List<double[]> source) {
        List<double[]> copy = new ArrayList<>(source.size());
        for (double[] centroid : source) {
            copy.add(new double[]{centroid[0], centroid[1]});
        }
        return copy;
    }

    private static String stringifyCentroids(List<double[]> centroids) {
        StringBuilder sb = new StringBuilder();
        for (double[] centroid : centroids) {
            if (sb.length() > 0) {
                sb.append(";");
            }
            sb.append(centroid[0]).append(",").append(centroid[1]);
        }
        return sb.toString();
    }

    private static double[] parsePoint(String raw) {
        String[] parts = raw.split(",");
        if (parts.length != 2) {
            return null;
        }
        try {
            double x = Double.parseDouble(parts[0].trim());
            double y = Double.parseDouble(parts[1].trim());
            return new double[]{x, y};
        } catch (NumberFormatException ex) {
            return null;
        }
    }
}
