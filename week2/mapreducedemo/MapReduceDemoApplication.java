package com.example.mapreducedemo;

import com.example.mapreducedemo.week2.PhoneTraffic;
import com.example.mapreducedemo.week2.TrafficStat;
import com.example.mapreducedemo.wordcount.WordCount2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@SpringBootApplication
public class MapReduceDemoApplication {
	// throws Exception
//	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//		Configuration conf = new Configuration();
//		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
//		String[] remainingArgs = optionParser.getRemainingArgs();
//		if ((remainingArgs.length != 2) && (remainingArgs.length != 4)) {
//			System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
//			System.exit(2);
//		}
//		Job job = Job.getInstance(conf, "word count");
//		job.setJarByClass(WordCount2.class);
//		job.setMapperClass(WordCount2.TokenizerMapper.class);
//		job.setCombinerClass(WordCount2.IntSumReducer.class);
//		job.setReducerClass(WordCount2.IntSumReducer.class);
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
//
//		List<String> otherArgs = new ArrayList<String>();
//		for (int i=0; i < remainingArgs.length; ++i) {
//			if ("-skip".equals(remainingArgs[i])) {
//				job.addCacheFile(new Path(remainingArgs[++i]).toUri());
//				job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
//			} else {
//				otherArgs.add(remainingArgs[i]);
//			}
//		}
//		FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
//		FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
//
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
//	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: TrafficStat <in> <out>");
			System.exit(2);
		}
		System.err.println("otherArgs: " + Arrays.toString(otherArgs));

		Job job = Job.getInstance(conf, "TrafficStat");
		job.setJarByClass(TrafficStat.class);
		job.setMapperClass(TrafficStat.TrafficMapper.class);
		job.setCombinerClass(TrafficStat.TrafficReducer.class);
		job.setReducerClass(TrafficStat.TrafficReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PhoneTraffic.class);

		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(otherArgs[otherArgs.length - 2]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
