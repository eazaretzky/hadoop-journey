package ar.com.orangebit.hadoop.examples.twitter;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ar.com.orangebit.hadoop.examples.twitter.domain.Tweet;

public class TweetMap extends Mapper<LongWritable, Tweet, Text, LongWritable> {

    private final static LongWritable ONE = new LongWritable(1);
    private Text word = new Text();

    @Override
    public void map(LongWritable offset, Tweet value, Context context)
            throws IOException, InterruptedException {
        word.set(value.getLang());
        context.write(word, ONE);
    }

}
