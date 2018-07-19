package WordCountProgramv3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountJob {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: WordCount Problem <input path> <output path>");
			System.exit(-1);
		}
		final long startTime = System.currentTimeMillis();
		// Initialize the Job object with configuration
		Job job = new Job();
		// setting the job name
		job.setJobName("WordCountProblem-v3");
		// hadoop jar command purpose, detecting the main job class
		job.setJarByClass(WordCountJob.class);
		// setting the custom mapper class
		job.setMapperClass(WordCountMapperv3.class);
		// setting the custom reducer class
		job.setReducerClass(WordCountReducerv3.class);

		job.setMapOutputKeyClass(Text.class); // Key2
		job.setMapOutputValueClass(LongWritable.class); // value2

		job.setOutputKeyClass(Text.class); // Key3
		job.setOutputValueClass(LongWritable.class); // Value3

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		final long duration = System.currentTimeMillis() - startTime;
		System.out.println("Total time ran" + duration);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
