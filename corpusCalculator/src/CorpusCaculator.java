package corpusCalculator;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class CorpusCaculator {

	/*Method and links for putting the file into the distributed cache format for the reducer of the
	third stage.
	
	Reference: https://developer.yahoo.com/hadoop/tutorial/module5.html
*/	
	
	public static final String LOCAL_CORPUS_ = "/opt/corpus.txt";
	/*../*/

	public static final String HDFS_CORPUS_ = "/data/corpus.txt";

	public void cacheFile(JobConf conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path hdfsPath = new Path(HDFS_CORPUS_);

		// upload the file to hdfs. Overwrite any existing copy.
		fs.copyFromLocalFile(false, true, new Path(LOCAL_CORPUS_),
				hdfsPath);
		DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
	}
	
	public static void main(String[] args) throws IOException, URISyntaxException {
		
		/*JOB --1*/ 
		JobConf conf = new JobConf(GetFrequency.class);
		conf.setJobName("GetFrequency");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(GetFrequency.Map.class);
		conf.setCombinerClass(GetFrequency.Reduce.class);
		conf.setReducerClass(GetFrequency.Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[1]));
		FileOutputFormat.setOutputPath(conf, new Path("corpusOut/output1"));
		JobClient.runJob(conf);
		
		/*JOB --2*/
		JobConf conf1 = new JobConf(ProbabiliyCalculator.class);
		conf1.setJobName("ProbabiliyCalculator");
		conf1.setMapOutputKeyClass(Text.class);
		conf1.setMapOutputValueClass(Text.class);
		conf1.setMapperClass(ProbabiliyCalculator.Map.class);
		conf1.setReducerClass(ProbabiliyCalculator.Reduce.class);
		conf1.setInputFormat(TextInputFormat.class);
		conf1.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf1, new Path("corpusOut/output1/part-00000"));
		FileOutputFormat.setOutputPath(conf1, new Path("corpusOut/op2"));
		JobClient.runJob(conf1);

		/*JOB --3*/
		JobConf conf2 = new JobConf(GetTopThree.class);
		conf2.setJobName("GetTopThree");
		conf2.setNumReduceTasks(1);
		conf2.setMapOutputKeyClass(Text.class);
		conf2.setMapOutputValueClass(Text.class);
		conf2.setMapperClass(GetTopThree.Map.class);
		conf2.setReducerClass(GetTopThree.Reduce.class);
		conf2.setInputFormat(TextInputFormat.class);
		conf2.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf2, new Path("corpusOut/op2/part-00000"));
		FileOutputFormat.setOutputPath(conf2, new Path("corpusOut/op3"));
		FileOutputFormat.setOutputPath(conf2, new Path(args[2]));
		new CorpusCaculator().cacheFile(conf2);

		JobClient.runJob(conf2);
		
		
	} 

}