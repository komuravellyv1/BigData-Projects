package WordCountProgramv3;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapperv3 extends Mapper<Object, Text, Text, LongWritable> {
	private Map<String, Integer> map;

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// An object that maps keys to values,each key can map to at most one value
		// Map<String, Integer> map = new HashMap<String, Integer>();
		Map<String, Integer> map = getMap();

		/*
		 * The string tokenizer class allows an application to break a string into
		 * tokens A token is nothing but a line in file
		 */
		StringTokenizer split = new StringTokenizer(value.toString());

		/*
		 * Tests if there are more tokens available from this tokenizer's string. If
		 * this method returns true, then a subsequent call to nextToken with no
		 * argument will successfully return a token.
		 */
	try {
		while (split.hasMoreTokens())

		{
			// gives the next token from this string tokenizer
			//String token = itr.nextToken();
			String ntoken = split.nextToken();
			if (map.containsKey(ntoken))// true if this map has a mapping for the specified key
			{
				int sum = map.get(ntoken) + 1;
				map.put(ntoken, sum);
			} else {
				map.put(ntoken, 1);
			}
		}
	}
	catch(Exception e)
	{
		
	}
	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Map<String, Integer> map = getMap();
		Iterator<Map.Entry<String, Integer>> iterations = map.entrySet().iterator();
		while (iterations.hasNext()) {
			Map.Entry<String, Integer> pair = iterations.next();
			context.write(new Text(pair.getKey()), new LongWritable(pair.getValue().intValue()));
		}
	}

	public Map<String, Integer> getMap() {
		if (null == map)
			map = new HashMap<String, Integer>();
		return map;
	}
}
