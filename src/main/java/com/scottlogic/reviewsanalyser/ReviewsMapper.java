package com.scottlogic.reviewsanalyser;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Our Mapper class.
 * @author David.Wybourn
 */
public class ReviewsMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {

        final Pattern productIdPattern = Pattern.compile("product/productId: (.*)\n");
        final Matcher productIdMatcher = productIdPattern.matcher(value.toString());

        if(productIdMatcher.find()) {
            final String productId = productIdMatcher.group(1);
            final Pattern reviewPattern = Pattern.compile(".*review/text: (.*)");
            final Matcher reviewMatcher = reviewPattern.matcher(value.toString());

            if(reviewMatcher.find()) {
                context.write(new Text(productId), new Text(reviewMatcher.group(1)));
            }
        }
    }
}
