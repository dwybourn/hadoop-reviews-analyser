package com.scottlogic.reviewsanalyser;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Review analyser main class.
 * @author David.Wybourn
 */
public class ReviewsAnalyser {

    /**
     * Private constructor to prevent instantiation of this class.
     */
    private ReviewsAnalyser() {
        // empty
    }

    /**
     * Our main method. Reads in the input and output paths from the command line and sets up the {@link Job} details.
     * @param args Command line arguments, should be two arguments consisting of input and output path
     * @throws IOException Thrown if the input path given does not exist.
     * @throws ClassNotFoundException Unable to find class
     * @throws InterruptedException Exception thrown if the running job was unexpectedly interrupted.
     */
    public static void main(final String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if(args.length != 2) {
            System.err.println("Need to set input and output path");
            System.exit(-1);
        }

        final Job job = createJob();

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * Creates a {@link Job}, sets the necessary input/output classes and tells the job to point to our mapper/reducer.
     * @return new job
     * @throws IOException Thrown if failed to create a new job.
     */
    private static Job createJob() throws IOException {
        final Configuration configuration = new Configuration(true);
        configuration.set("textinputformat.record.delimiter", "\n\n");

        final Job job = new Job(configuration);
        job.setJarByClass(ReviewsAnalyser.class);
        job.setJobName("Reviews Analyser");

        job.setMapperClass(ReviewsMapper.class);
        job.setReducerClass(ReviewsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        return job;
    }

}
