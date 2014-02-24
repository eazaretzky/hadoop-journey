package globantU.examples.twitter;

import globantU.examples.twitter.domain.Tweet;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TweetMap extends Mapper<LongWritable, Tweet, Text, LongWritable> {

  private final static LongWritable ONE = new LongWritable(1);
  private Text word = new Text();
  private final static List<String> KEYWORDS = Arrays.asList("google", "disney", "facebook",
      "love", "the", "cool");

  @Override
  public void map(LongWritable offset, Tweet value, Context context) throws IOException,
      InterruptedException {
    for (String keyword : KEYWORDS) {
      if (value.getText().contains(keyword)) {
        word.set(keyword);
        context.write(word, ONE);
      }
    }
  }

}
