package corpusCalculator;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class GetTopThree extends Configured implements Tool {

	public static class Map extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {
		Text prob = new Text();
		Text send = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
						throws IOException {
			send.set("continue");
			prob.set(value.toString().trim());
			output.collect(send, prob);
		}
	}

	public static class Reduce extends MapReduceBase implements
	Reducer<Text, Text, Text, DoubleWritable> {
		
		//sentenceBucket stores all the sentences from the file.
		LinkedHashSet<String> sentenceBucket = new LinkedHashSet<String>();
		Hashtable<String, Double> ht = new Hashtable<String, Double>();

		public void populateSentenceBuket(Path cachepath) throws IOException {
			BufferedReader wordReader = new BufferedReader(new FileReader(
					cachepath.toString()));
			try {
				String s;
				while ((s = wordReader.readLine()) != null) {
					sentenceBucket.add(s.trim());
				}
			} finally {
				wordReader.close();
			}
		}

		public void configure(JobConf job) {
			try {
				System.out.println("Inside configure!!");
				//String CorpusCacheName = new Path(CorpusCaculator.HDFS_CORPUS_).getName();
				Path [] cacheFiles = DistributedCache.getLocalCacheFiles(job);
				if (null != cacheFiles && cacheFiles.length > 0) {
					for (Path cachePath : cacheFiles) {
						//if (cachePath.getName().equals(CorpusCacheName)) {
							populateSentenceBuket(cachePath);
							break;
						//}
					}
				}
				System.out.println("Populated the sentence bucket with + " + sentenceBucket.size() + " entries----");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, DoubleWritable> output, Reporter reporter)
						throws IOException {
			//Arrays to maintain the top 3 sentences and their probabilites. 
			String[] sentenceArray = { "sent1", "sent2", "sent3" };
			Double[] probabilityAray = { 0.0, 0.0, 0.0 };
			
			while (values.hasNext()) {
				String temp = values.next().toString().trim();
				ht.put(temp.split("\t")[0],
						Double.parseDouble(temp.split("\t")[1]));
			}

			System.out.println("Populated the hashtable -- " + ht.size());

			Iterator<String> iter = sentenceBucket.iterator();

			while (iter.hasNext()) {
				Double probsentence = 1.0;
				int wc = 1;
				String line = iter.next().toString().toLowerCase().trim();
				String[] words = new String[] {};
				if (line != null && line != "")
					words = line.split(" +");
				for (String w : words) {
					if (ht.containsKey(w.trim() + " " + wc)) {
						Double prob = ht.get(w + " " + wc);
						probsentence *= ((double) prob / 100000);
					}
					wc = wc + 1;
				}
				
				//Manage the arrays to get the top three sentences. 
				if (probsentence > probabilityAray[0]) {
					probabilityAray[2] = probabilityAray[1];
					sentenceArray[2] = sentenceArray[1];

					probabilityAray[1] = probabilityAray[0];
					sentenceArray[1] = sentenceArray[0];

					probabilityAray[0] = probsentence;
					sentenceArray[0] = line;
				}

				else if (probsentence > probabilityAray[1]) {
					probabilityAray[2] = probabilityAray[1];
					sentenceArray[2] = sentenceArray[1];

					probabilityAray[1] = probsentence;
					sentenceArray[1] = line;
				}

				else if (probsentence > probabilityAray[2]) {
					probabilityAray[2] = probsentence;
					sentenceArray[2] = line;
				}

			}

			Text toBeSent = new Text();
			for (int i = 0; i < 3; i++) {
				toBeSent.set(sentenceArray[i]);
				output.collect(toBeSent, new DoubleWritable(probabilityAray[i]));
			}

		}
	}

	public int run(String[] args) throws Exception {
		return 0;
	}
}
