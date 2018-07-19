package WordCountProgramsw;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducersw extends Reducer<Text, IntWritable, Text, LongWritable> {
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		// sum the list of values
		long sum = 0;
		for (IntWritable value : values) {
			sum = sum + value.get();
		}
		context.write(key, new LongWritable(sum)); // assign the sum to corresponding word or key
	}

}
