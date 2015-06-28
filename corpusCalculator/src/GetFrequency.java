package corpusCalculator;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class GetFrequency extends Configured implements Tool {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		Hashtable<Integer,Integer> ht=new Hashtable<Integer,Integer>();
		private final static IntWritable one = new IntWritable(1);
		private Text token = new Text();
		//Gets the frequency of the token + location of its occurence. 
		
		public void map(LongWritable key, Text value,  OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String line = value.toString().trim();
			//String line = value.toString().trim().replaceAll("[^A-Za-z0-9]", " ");
			int locationCounter =0;
			StringTokenizer tokenizer = new StringTokenizer(line," ");
			while (tokenizer.hasMoreTokens()) {
				locationCounter ++;
				token.set(tokenizer.nextToken()+ " " +locationCounter );
				output.collect(token, one);
			}
		}
	} 
	public static class Reduce  extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sumFrequency = 0;
			while (values.hasNext()) {
				sumFrequency += values.next().get();
			}
			output.collect(key, new IntWritable(sumFrequency));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		return 0;
	}
}