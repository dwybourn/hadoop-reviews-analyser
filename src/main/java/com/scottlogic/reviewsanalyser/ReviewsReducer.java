package com.scottlogic.reviewsanalyser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Our reducer classes. Takes in {@link Text} as an input for key and value, output is a {@link Text} key and a {@link IntWritable}.
 * @author David.Wybourn
 */
public class ReviewsReducer extends Reducer<Text, Text, Text, IntWritable> {

    /** Map containing the lengths of all the reviews. */
    final Map<Text, List<Integer>> reviewsLengths = new HashMap<Text, List<Integer>>();

    @Override
    public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {

        List<Integer> reviewLengths = reviewsLengths.get(key);

        if(reviewLengths == null) {
            reviewLengths = new ArrayList<Integer>();
            reviewsLengths.put(key, reviewLengths);
        }

        for(final Text review : values) {
            reviewLengths.add(review.toString().length());
        }

        context.write(key, new IntWritable(getAverage(reviewLengths)));
    }

    /**
     * Gets the average value of a list of integers. Result is round to nearest integer.
     * @param values List of integers to get the average for.
     * @return rounded value of the average of the input
     */
    private Integer getAverage(final List<Integer> values) {
        Integer totalValue = 0;
        for(final Integer value : values) {
            totalValue += value;
        }

        return Math.round(totalValue/values.size());
    }
}
