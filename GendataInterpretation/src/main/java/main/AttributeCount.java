package main;

import java.io.IOException;

import mapper.AttributeCountMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import reducer.AttributeCountReducer;

public class AttributeCount {
	public static void main(String[] args) throws IOException,
	InterruptedException, ClassNotFoundException {
		Path input = new Path(args[0]);
		// Path temp1 = new Path(args[1]);
		Path output = new Path(args[1]);

		Job job = new Job(new Configuration(), "COUNT ATTRIBUTE");

		FileSystem fs = FileSystem.get(job.getConfiguration());
		// get the FileStatus list from given dir
		FileStatus[] status_list = fs.listStatus(input);
		if (status_list != null) {
			for (FileStatus status : status_list) {
				// add each file to the list of inputs for the map-reduce job
				FileInputFormat.addInputPath(job, status.getPath());
			}
		}

		FileOutputFormat.setOutputPath(job, output);
		job.setJarByClass(AttributeCount.class);

		job.setMapperClass(AttributeCountMapper.class);
		job.setCombinerClass(AttributeCountReducer.class);
		job.setReducerClass(AttributeCountReducer.class);
		job.setNumReduceTasks(5);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.waitForCompletion(true);

	}
}
