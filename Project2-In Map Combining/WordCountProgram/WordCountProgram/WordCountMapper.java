package WordCountProgram;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	public void map(LongWritable Key, Text value, Context context) throws IOException,InterruptedException
	{
		String line = value.toString(); //read the file line by line
		String[] words = line.split(" "); // split the line into words
		for (String word : words) // assign count 1 to each word
		{
			context.write(new Text(word), new LongWritable(1));

		}

	}

}
