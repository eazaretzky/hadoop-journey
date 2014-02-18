package ar.com.orangebit.hadoop.examples.twitter.io;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import ar.com.orangebit.hadoop.examples.twitter.domain.Tweet;

public class TweetFileInputFormat extends FileInputFormat<LongWritable, Tweet>{

    @Override
    public RecordReader<LongWritable, Tweet> createRecordReader(InputSplit arg0, TaskAttemptContext arg1)
            throws IOException, InterruptedException {
        return new TweetRecordReader();
    }
}
