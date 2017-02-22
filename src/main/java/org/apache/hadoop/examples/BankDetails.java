package org.apache.hadoop.examples;

import java.io.IOException;
//import java.util.Collections;
//import java.util.List;
//import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class BankDetails {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		  // mapper-input:offset, String mapper-output:String, int

		  private static final String RECORD_FOOTER_REGEX = "^09\\s[0-9]{2,2}$";
		  private static final String RECORD_HEADER_REGEX = "^08\\s[0-9]{2,2}$";
		  private Text word = new Text();
		  private IntWritable medal = new IntWritable();
		  private StringBuilder recordBuilder;

		  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		   System.out.println("--> In the mapper. line = " + value.toString());
		   String line = value.toString();
		   if (line.matches(RECORD_HEADER_REGEX)) {
		    // record is starting initialize recode
		    recordBuilder = new StringBuilder();
		    return;
		   }
		   if (line.matches(RECORD_FOOTER_REGEX)) {
		    // record is ending
		    word.set(recordBuilder.toString());
		    context.write(word, medal);
		    // System.out.println(key + " " + value + " " + word + " " +
		    // medal);
		    return;
		   }
		   if (recordBuilder == null) {
		    throw new IllegalStateException("Header should be defined before record strats");
		   }
		   recordBuilder.append(line).append(" ");
		   // System.out.println(key + " " + value + " " + word + " " + medal);
		  }
		 }

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		// private IntWritable result = new IntWritable();
		//static int max = 0;
		private Text maxWord = new Text();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int sum = 1;

			System.out.println("--> In the reducer. key = " + key.toString());

			for (IntWritable val : values) {
				sum += val.get();
			}

			maxWord.set(key);

			// result.set(max);

			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {

		// Load the configuration files and
		// add them to the the conf object
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		// The input and the output path have to sent to the main/driver program
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}

		// Create a job object and give the job a name
		// It allows the user to configure the job, submit it,
		// control its execution, and query the state.
		Job job = new Job(conf, "word count");

		// Specify the jar which contains the required classes
		// for the job to run.
		job.setJarByClass(Medals.class);

		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		// job.setMapSpeculativeExecution(false);
		// job.setReduceSpeculativeExecution(false);

		// Set the output key and the value class for the entire job
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Set the Input (format and location) and similarly for the output also
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		job.setNumReduceTasks(1);// setting number of reducer

		// Submit the job and wait for it to complete
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
