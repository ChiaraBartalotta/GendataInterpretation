package mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OrderValueMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] components = line.split("\t");
		String productsCouple = components[0];
		long quantity = Long.parseLong(components[1]);
		context.write(new LongWritable(quantity*(-1)), new Text(productsCouple));
	}

}
