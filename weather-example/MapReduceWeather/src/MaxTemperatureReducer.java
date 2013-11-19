import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer extends
    Reducer<Text, IntWritable, Text, IntWritable> {

  @Override
  public void reduce(final Text key, final Iterable<IntWritable> values,
      final Context context) throws IOException, InterruptedException {
    int maxValue = Integer.MIN_VALUE;

    for (final IntWritable value : values) {
      maxValue = Math.max(maxValue, value.get());
    }
    context.write(key, new IntWritable(maxValue));
  }
}
