package globantU.examples.twitter;

import globantU.examples.twitter.io.TweetFileInputFormat;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class MapReduce {

  private Job job;

  public MapReduce(Job job) {
    this.job = job;
  }

  public static void main(String[] args) throws Exception {
    MapReduce mapReduce = new MapReduce(Job.getInstance());
    mapReduce.run(args);
  }

  public boolean run(String[] args) throws ClassNotFoundException, IOException,
      InterruptedException {
    FileInputFormat.addInputPath(job, new Path("/in"));
    FileOutputFormat.setOutputPath(job, new Path("/out"));

    job.setJarByClass(MapReduce.class);
    job.setMapperClass(TweetMap.class);
    job.setCombinerClass(TweetReducer.class);
    job.setReducerClass(TweetReducer.class);
    job.setInputFormatClass(TweetFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    return job.waitForCompletion(true);
  }
}
