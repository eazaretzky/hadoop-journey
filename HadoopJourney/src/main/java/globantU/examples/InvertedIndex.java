package globantU.examples;
//package ar.com.orangebit.hadoop.examples;
//
//import java.io.IOException;
//import java.util.Arrays;
//import java.util.HashSet;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Set;
//
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.FileInputFormat;
//import org.apache.hadoop.mapred.FileOutputFormat;
//import org.apache.hadoop.mapred.FileSplit;
//import org.apache.hadoop.mapred.JobClient;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.MapReduceBase;
//import org.apache.hadoop.mapred.Mapper;
//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Reducer;
//import org.apache.hadoop.mapred.Reporter;
//import org.apache.hadoop.mapred.TextInputFormat;
//import org.apache.hadoop.mapred.TextOutputFormat;
//
//public class InvertedIndex {
//
//	public static class Map extends MapReduceBase implements
//			Mapper<LongWritable, Text, Text, Text> {
//		
//		private Text word = new Text();
//
//		public void map(LongWritable key, Text value,
//				OutputCollector<Text, Text> output, Reporter reporter)
//				throws IOException {
//			
//			List<String> wordsInLine = Arrays.asList(value.toString()
//					.replaceAll("\\p{P}", "").split(" "));
//			
//			FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
//			String fileName = fileSplit.getPath().getName();
//			
//			for (String stringWord : wordsInLine) {
//				word.set(stringWord);
//					output.collect(word, new Text(fileName));
//			}
//		}
//
//	}
//
//	public static class Reduce extends MapReduceBase implements
//			Reducer<Text, Text, Text, Text> {
//		public void reduce(Text key, Iterator<Text> values,
//				OutputCollector<Text, Text> output, Reporter reporter)
//				throws IOException {
//			
//			Set<Text> books = new HashSet<Text>();
//			
//			while (values.hasNext()) {
//				books.add(values.next());
//			}
//			output.collect(key, new Text(books.toString()));
//		}
//	}
//
//	public static void main(String[] args) throws Exception {
//		JobConf conf = new JobConf(InvertedIndex.class);
//		conf.setJobName("inverted index");
//
//		conf.setOutputKeyClass(Text.class);
//		conf.setOutputValueClass(Text.class);
//
//		conf.setMapperClass(Map.class);
//		conf.setCombinerClass(Reduce.class);
//		conf.setReducerClass(Reduce.class);
//
//		conf.setInputFormat(TextInputFormat.class);
//		conf.setOutputFormat(TextOutputFormat.class);
//
//		FileInputFormat.setInputPaths(conf, new Path(args[0]));
//		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
//
//		JobClient.runJob(conf);
//	}
//}