package first;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRankDriver extends Configured implements Tool {

	private static final int MAX_ITERATIONS = 20;
	
	@Override
	public int run(String[] args) throws Exception {
		/*
		 * Validate that three arguments were passed from the command line.
		 */
		if (args.length != 3) {
			System.out.printf("Usage: PageRankDriver <input dir> <output dir> <alpha>\n");
			System.exit(-1);
		}

		Configuration conf = getConf();
		conf.set("mapreduce.client.genericoptionsparser.used", "true");
		
		// set the alpha in the conf
		conf.set("alpha", args[2]);
		
		// get the adjacency lists
		Job preJob = Job.getInstance(conf);
		preJob.setInputFormatClass(TextInputFormat.class);
		preJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(preJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(preJob, new Path(args[1] + "/v0"));
		preJob.setOutputKeyClass(Text.class);
		preJob.setOutputValueClass(GraphNode.class);
		preJob.setMapOutputKeyClass(Text.class);
		preJob.setMapOutputValueClass(Text.class);
		preJob.setMapperClass(AdjacencyListMapper.class);
		preJob.setReducerClass(AdjacencyListReducer.class);
		preJob.setJarByClass(PageRankDriver.class);
		preJob.setJobName("AdjacencyList");
		preJob.waitForCompletion(true);
		
		int i = 0;
		long unstable_nodes = 1;
		
		// it goes until all the nodes pageRanks respect the alpha threshold
		// MAX_ITERATIONS is an upper limit for the number of iterations,
		// because nobody likes infinite loops
		while(unstable_nodes > 0 && i < MAX_ITERATIONS){
			Job job = Job.getInstance(conf);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.setInputPaths(job, new Path(args[1] + "/v" + String.valueOf(i) +"/"));
			FileOutputFormat.setOutputPath(job, new Path(args[1] + "/v" + String.valueOf(i + 1) +"/"));
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(GraphNode.class);
			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);
			job.setJarByClass(PageRankDriver.class);
			job.setJobName("PageRank");
			job.waitForCompletion(true);
			
			// check the pageRank delta
			Counters counters = job.getCounters();
			unstable_nodes = counters.findCounter(PageRankConvergence.UNSTABLE).getValue();
			i++;
		}
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new PageRankDriver(), args);
		System.exit(exitCode);
	}
}
