package ar.com.orangebit.hadoop.examples.twitter;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TweetReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private static final LongWritable QUANTITY = new LongWritable();

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0L;
        for (LongWritable value : values) {
            sum += value.get();
        }
        QUANTITY.set(sum);
        context.write(key, QUANTITY);
    }
}
