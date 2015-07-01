package main;

import java.io.IOException;

import mapper.AttributeValueCountMapper;
import mapper.OrderValueMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import reducer.CountReducer;
import reducer.OrderValueReducer;

public class AttributeValueCount {
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Path input = new Path(args[0]);
		Path temp1 = new Path("temp");
		Path output = new Path(args[1]);

		Job job = new Job(new Configuration(), "COUNT ATTRIBUTE");

		FileSystem fs = FileSystem.get(job.getConfiguration());
		FileStatus[] status_list = fs.listStatus(input);
		if (status_list != null) {
			for (FileStatus status : status_list) {
				// add each file to the list of inputs for the map-reduce job
				FileInputFormat.addInputPath(job, status.getPath());
			}
		}

		FileOutputFormat.setOutputPath(job, temp1);
		job.setJarByClass(AttributeValueCount.class);

		job.setMapperClass(AttributeValueCountMapper.class);
		job.setCombinerClass(CountReducer.class);
		job.setReducerClass(CountReducer.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		boolean esit = job.waitForCompletion(true);

		if (esit) {
			Job job2 = new Job(new Configuration(), "ORDER COUNT PRODUCT");
			FileInputFormat.setInputPaths(job2, temp1);
			FileOutputFormat.setOutputPath(job2, output);
			job2.setJarByClass(AttributeValueCount.class);
			job2.setMapperClass(OrderValueMapper.class);
			job2.setReducerClass(OrderValueReducer.class);
			job2.setMapOutputKeyClass(LongWritable.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(LongWritable.class);
			job2.waitForCompletion(true);

		} else
			System.exit(1);

	}
}
