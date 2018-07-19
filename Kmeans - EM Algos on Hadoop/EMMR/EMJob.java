package EMMR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Random;

public class EMJob extends Configured implements Tool {

    public static final Integer DIMENSIONAL = 4;
    public static final Integer CLASTER_COUNT = 3;
    public static HashMap<Double[], Integer> centers = new HashMap<>();

    private static Double[] createRandomPoint() {
        Random r = new Random();
        Double[] point = new Double[DIMENSIONAL];
        for (int i = 0; i < DIMENSIONAL; i++) {
            point[i] = r.nextDouble() * 6 + 1;
        }
        return point;
    }

    private static Double[] createPoint(String[] s) {
        Double[] point = new Double[DIMENSIONAL];
        for (int i = 0; i < DIMENSIONAL; i++) {
            point[i] = Double.parseDouble(s[i]);
        }
        return point;
    }

    public void configure(String fileName, int iteration) throws IOException {
        centers.clear();
        if (iteration == 0) {
            centers.put(createRandomPoint(), 50);
            centers.put(createRandomPoint(), 50);
            centers.put(createRandomPoint(), 50);
        } else {
            BufferedReader br = new BufferedReader(new FileReader(new File(fileName + iteration + "/part-r-00000")));
            String next = br.readLine();
            while (next != null && !next.equals("")) {
                centers.put(createPoint(next.split("\t")[0].split(",")), 50);
                next = br.readLine();
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        int iterationCount = 0; // counter to set the ordinal number of the intermediate outputs
        Job job;
        String jobName = "emjob";

        // while there are more gray nodes to process
        while (iterationCount <= 2) {
            String input, output;
            job = new Job(new Configuration(), jobName);
            configure(args[1], iterationCount);

            job.setMapperClass(EMMapper.class);
            job.setReducerClass(EMReducer.class);

            //the number of reducers is set to 2, this can be altered according to the program's requirements
            job.setNumReduceTasks(2);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

//	        job.setInputFormatClass(TextInputFormat.class);  //default already
//	        job.setOutputFormatClass(TextOutputFormat.class);

            job.setJarByClass(EMJob.class);

            //setting the input file and output file for each iteration
            //during the first time the user-specified file will be the input whereas for the subsequent iterations
            // the output of the previous iteration will be the input
            input = args[0];


            output = args[1] + (iterationCount + 1); // setting the output file

            FileInputFormat.setInputPaths(job, new Path(input)); // setting the input files for the job
            FileOutputFormat.setOutputPath(job, new Path(output)); // setting the output files for the job

            job.waitForCompletion(true); // wait for the job to complete

            Counters jobCntrs = job.getCounters();
            iterationCount++;

        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        args = new String[]{"iris.data", "output/em"};
        int res = ToolRunner.run(new Configuration(), new EMJob(), args);
        if (args.length != 2) {
            System.err.println("Usage: <in> <output name> ");
        }
        System.exit(res);
    }
}
