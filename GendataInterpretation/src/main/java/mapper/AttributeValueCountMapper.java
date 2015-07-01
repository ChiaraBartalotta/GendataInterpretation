package mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class AttributeValueCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	private static long ONE_NUMBER = 1;
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		//StringTokenizer tokenizer = new StringTokenizer(line, " \t");
		String[] splitEle = line.split("\t");
		context.write(new Text(splitEle[0]+"="+splitEle[1]), new LongWritable(ONE_NUMBER));

	}

}
