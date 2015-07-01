package reducer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OrderValueReducer extends  Reducer<LongWritable, Text, Text, LongWritable> {
	
	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for(Text text : values)
			context.write(new Text(text.toString()), new LongWritable(key.get()*(-1)));
	}
}
