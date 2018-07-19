package WordCountProgramv2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapperv2 extends Mapper<Object, Text, Text, LongWritable> {
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// An object that maps keys to values,each key can map to at most one value
		Map<String, Integer> map = new HashMap<String, Integer>();

		/*
		 * The string tokenizer class allows an application to break a string into
		 * tokens A token is nothing but a line in file
		 */
		StringTokenizer splits = new StringTokenizer(value.toString());

		/*
		 * Tests if there are more tokens available from this tokenizer's string. If
		 * this method returns true, then a subsequent call to nextToken with no
		 * argument will successfully return a token.
		 */
		boolean mtokens = splits.hasMoreTokens();

		try {
			
		
		while (mtokens)

		{
			String ntoken = splits.nextToken();// gives the next token from this string tokenizer
			if (map.containsKey(ntoken))// true if this map has a mapping for the specified key
			{
				int sum = map.get(ntoken) + 1;
				map.put(ntoken, sum);
			} else {
				map.put(ntoken, 1);
			}
		}
		}
		catch(Exception e) {
			
		}
		Iterator<Map.Entry<String, Integer>> iterations = map.entrySet().iterator();
		while (iterations.hasNext()) {
			Map.Entry<String, Integer> pair = iterations.next();
			context.write(new Text(pair.getKey()), new LongWritable(pair.getValue().intValue()));
		}
	}
}
