import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Program extends Configured implements Tool {

	private static List<Double[]> centerValues = new ArrayList<Double[]>();
	
	public static enum State {
		COUNTER;
	}

	@Override
	public int run(String[] args) throws Exception {

		if (args.length != 4) {
			System.err.println("Usage:-- <input file> <input table name> <center table name> <K points>");
			System.exit(-1);
		}

		/*
		 * Configuration config1 = new Configuration(); Job job = new
		 * Job(config1); job.setJobName("Convert Text");
		 * job.setJarByClass(Mapper.class);
		 * 
		 * job.setMapperClass(Mapper.class); job.setReducerClass(Reducer.class);
		 * 
		 * // increase if you need sorting or a special number of files
		 * job.setNumReduceTasks(1);
		 * 
		 * job.setOutputKeyClass(LongWritable.class);
		 * job.setOutputValueClass(Text.class);
		 * 
		 * job.setOutputFormatClass(SequenceFileOutputFormat.class);
		 * job.setInputFormatClass(TextInputFormat.class);
		 * 
		 * TextInputFormat.addInputPath(job, new Path(args[0]));
		 * SequenceFileOutputFormat.setOutputPath(job, new Path("/apps"));
		 * 
		 * // submit and wait for completion job.waitForCompletion(true);
		 */
		
		//Getting all the arguments
		long startTime = System.currentTimeMillis();
		String inputPathLocation = args[0];
		String inputTableName = args[1];
		String centerTableName = args[2];
		int k = Integer.parseInt(args[3]);
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("inputTableName", inputTableName);
		conf.set("centerTableName", centerTableName);
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		// Clean data for new iteration.
		HbaseUtil.deleteTable(admin, inputTableName);
		HbaseUtil.deleteTable(admin, centerTableName);
		// Create input and center table
		HbaseUtil.createTable(admin, inputTableName);
		HbaseUtil.createTable(admin, centerTableName);

		HTable center = new HTable(conf, centerTableName);
		
		Job dataLoadJob = new Job(conf, "Data Load into HBase");
		dataLoadJob.setJarByClass(Loader.class);
		FileInputFormat.setInputPaths(dataLoadJob, new Path(inputPathLocation));
		dataLoadJob.setInputFormatClass(TextInputFormat.class);
		dataLoadJob.setMapperClass(Loader.LoaderMapper.class);

		TableMapReduceUtil.initTableReducerJob(inputTableName, null, dataLoadJob);
		dataLoadJob.setNumReduceTasks(0);
		dataLoadJob.waitForCompletion(true);

		/***Loading K initial values from the center table into local map*/
		Program.centerValues = HbaseUtil.getAllScans(inputTableName, k);
		HbaseUtil.putStartingPoints(center, k, centerValues);
		
		//iterative MR jobs...
		
		boolean continueflag = true;
		while (continueflag) {
			Job clusterJob = new Job(conf, "Running K-Means");
			clusterJob.setJarByClass(KMeans.class);
			Scan scan = new Scan();
			scan.setCaching(500);
			scan.setCacheBlocks(false);
			
			TableMapReduceUtil.initTableMapperJob(args[1], scan,
					KMeans.KMeansMapper.class, Text.class,
					Result.class, clusterJob);

			TableMapReduceUtil.initTableReducerJob(args[1],
					KMeans.KMeansReducer.class, clusterJob);

			clusterJob.setNumReduceTasks(k);
			clusterJob.waitForCompletion(true);

			// Check the counter value;
			long updatedCounter = clusterJob.getCounters().findCounter(Program.State.COUNTER).getValue();
			System.out.println("Counter value --- "+ updatedCounter);
			continueflag = updatedCounter > 0;
		}
		long endTime = System.currentTimeMillis();
		System.out.println("Execution time = " + (endTime-startTime));
		return 0;
	}

	public static void main(String[] args) throws Exception {
		Program driver = new Program();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}
