import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.List;
import java.io.IOException;
import java.util.ArrayList;

//Class to load Data into the HBase table. 
public class Loader {
	public static class LoaderMapper extends
	Mapper<LongWritable, Text, Text, Put> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			Put put = new Put(Bytes.toBytes(key.get()));
			if (line != "") {
				String[] lineArray = line.replaceAll("\\s+", " ").trim()
						.split(" ");
				// System.out.println("What I see as a lineArr" + lineArr);
				List<Double> values = new ArrayList<Double>();
				for (String s : lineArray) {
					values.add(Double.parseDouble(s)); // build the list of
					// values...
				}
				if (lineArray.length >= 10) {
					HbaseUtil.buildPutForRow(put, values);
				} else {
					System.out.println("Incorrect Data Format");
				}
				context.write(new Text(key.toString()), put);
			}
		}
	}
}
