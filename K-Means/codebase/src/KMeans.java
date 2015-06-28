import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class KMeans {

	public static class KMeansMapper extends TableMapper<Text, Result> {
		private Map<String, KeyValue[]> centers;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			String inputTableName = conf.get("centerTableName");
			centers = HbaseUtil.loadFromHTable(inputTableName);
			super.setup(context);
		}
		
		public void map(ImmutableBytesWritable row, Result value, Context context)
			throws IOException, InterruptedException {
			String centerID = HbaseUtil.findNearestCenter(centers, value.raw());
			context.write(new Text(centerID), value);
		}
	}
	
	public static class KMeansReducer extends TableReducer<Text, Result, ImmutableBytesWritable> {
		private Map<String, KeyValue[]> centers;
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			String centerTableName = conf.get("centerTableName");
			centers = HbaseUtil.loadFromHTable(centerTableName);
			super.setup(context);
		}
		
		public void reduce(Text key, Iterable<Result> results, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String centerTableName = conf.get("centerTableName");
			List<Double> center = HbaseUtil.findAverage(results);
			//System.out.println("Trying to get from the centers -- " + key.toString());
			KeyValue[] oldCenter = centers.get(key.toString());
			Double distance = HbaseUtil.distance(center, oldCenter);
			//System.out.println("The distance calculated is -- " + distance);
			if (distance > 0.01) {
	
				Configuration config = HBaseConfiguration.create();
				HTable centerTable = new HTable(config, centerTableName);
				Put put = new Put(Bytes.toBytes(key.toString()));
				HbaseUtil.buildPutForRow(put, center);
				centerTable.put(put);
				
				//increment the global counter.
				context.getCounter(Program.State.COUNTER).increment(1);
				centerTable.close();
				context.write(null, put);				
			}
		}
	}
}
