package ar.com.orangebit.hadoop.examples.twitter.io;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.log4j.Logger;
import org.json.JSONException;

import ar.com.orangebit.hadoop.examples.twitter.domain.Tweet;

public class TweetRecordReader extends RecordReader<LongWritable, Tweet> {

    private static final Logger logger = Logger.getLogger(TweetRecordReader.class);
    
    private LineRecordReader lineRecordReader;
    private Tweet tweet = new Tweet();
    private boolean hasTweet = false;

    @Override
    public void close() throws IOException {
        this.lineRecordReader.close();
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return this.lineRecordReader.getCurrentKey();
    }

    @Override
    public Tweet getCurrentValue() throws IOException, InterruptedException {
        if (this.hasTweet) {
            return tweet;
        }
        return null;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return this.lineRecordReader.getProgress();
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException,
            InterruptedException {
        this.lineRecordReader = new LineRecordReader();
        this.lineRecordReader.initialize(inputSplit, taskAttemptContext);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        this.hasTweet = false;

        while (!this.hasTweet && this.lineRecordReader.nextKeyValue()) {
            Text currentValue = this.lineRecordReader.getCurrentValue();
            try {
                this.tweet.loadTweet(currentValue.toString());
                this.hasTweet = true;
            } catch (JSONException e) {
                logger.warn(String.format("Invalid JSON [%s]", currentValue), e);
            }
        }
        return this.hasTweet;
    }

}
