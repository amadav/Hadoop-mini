package corpusCalculator;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class ProbabiliyCalculator extends Configured implements Tool {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public Text position = new Text();
		public Text keyPairValue = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String line = value.toString().trim();
			String[] resultString = line.split("\t");
			String tokenName = resultString[0].split(" ")[0];
			String pos = resultString[0].split(" ")[1];
			String frequency = resultString[1];
			String keyValue = tokenName + " " + frequency;
			position.set(pos);
			keyPairValue.set(keyValue);
			output.collect(position, keyPairValue);

		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, DoubleWritable> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			Hashtable<String, Integer> htable = new Hashtable<String, Integer>();
			while (values.hasNext()) {
				String temp = values.next().toString();
				htable.put(temp.split(" ")[0], Integer.parseInt(temp.split(" ")[1]));
				sum = sum + Integer.parseInt(temp.split(" ")[1]);
			}
			for (Entry<String, Integer> entry : htable.entrySet()) {
				Text tobeSent = new Text();
				String str = entry.getKey().toLowerCase().trim();
				String keytohash = str + " " + key.toString().trim();
				tobeSent.set(keytohash);
				Integer value = entry.getValue();
				Double prob = (double) value / sum;
				Double  resultingProbability = (prob * 100000); //chosen factor
				output.collect(tobeSent, new DoubleWritable(resultingProbability));
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		return 0;
	}
}