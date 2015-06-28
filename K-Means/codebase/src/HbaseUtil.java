import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseUtil {

	public static void deleteTable(HBaseAdmin admin, String tableName)
			throws IOException {
		System.out.println("Deleting table - " + tableName);
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
	}

	public static void createTable(HBaseAdmin admin, String tableName)
			throws IOException {
		System.out.println("Creating table - " + tableName);
		HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		tableDesc.addFamily(new HColumnDescriptor("Area"));
		tableDesc.addFamily(new HColumnDescriptor("Property"));
		admin.createTable(tableDesc);
	}

	public static void putStartingPoints(HTable table, Integer k,
			List<Double[]> centerVals) throws IOException {

		for (int i = 0; i < k; i++) {
			Double[] doubleList = centerVals.get(i);
			HbaseUtil.addRecord(table, "center" + i, "Area", "X1", new String(
					doubleList[0].toString()));
			HbaseUtil.addRecord(table, "center" + i, "Area", "X5", new String(
					doubleList[1].toString()));
			HbaseUtil.addRecord(table, "center" + i, "Area", "X6", new String(
					doubleList[2].toString()));
			HbaseUtil.addRecord(table, "center" + i, "Area", "Y1", new String(
					doubleList[3].toString()));
			HbaseUtil.addRecord(table, "center" + i, "Area", "Y2", new String(
					doubleList[4].toString()));
			HbaseUtil.addRecord(table, "center" + i, "Property", "X2",
					new String(doubleList[5].toString()));
			HbaseUtil.addRecord(table, "center" + i, "Property", "X3",
					new String(doubleList[6].toString()));
			HbaseUtil.addRecord(table, "center" + i, "Property", "X4",
					new String(doubleList[7].toString()));
			HbaseUtil.addRecord(table, "center" + i, "Property", "X7",
					new String(doubleList[8].toString()));
			HbaseUtil.addRecord(table, "center" + i, "Property", "X8",
					new String(doubleList[9].toString()));
		}
	}

	public static List<Double[]> getAllScans(String tableNanme, int k)
			throws IOException {
		int numOfPoints = 0;
		Configuration config = HBaseConfiguration.create();
		HTable table = new HTable(config, tableNanme);
		Scan scan = new Scan();

		List<Double[]> centerVals = new ArrayList<Double[]>();
		ResultScanner scanner = table.getScanner(scan);
		for (Result result = scanner.next(); result != null; result = scanner
				.next()) {
			numOfPoints++;
			Double[] vals = new Double[10];
			int i = 0;
			for (KeyValue kv : result.raw()) {
				/*
				 * System.out.print("ROW " + kv.getRow().toString());
				 * System.out.print("FAMILY " + kv.getFamily().toString());
				 * System.out.print("Qualifier " +
				 * kv.getQualifier().toString());
				 */
				vals[i++] = Double.parseDouble(new String(kv.getValue()));
			}
			System.out.println(Arrays.toString(vals));
			centerVals.add(vals);
			if (numOfPoints == k)
				break;

		}
		scanner.close();
		table.close();
		return centerVals;
	}

	public static void buildPutForRow(Put put, List<Double> values) {
		put.add(Bytes.toBytes("Area"), Bytes.toBytes("X1"),
				Bytes.toBytes(values.get(0).toString()));
		put.add(Bytes.toBytes("Area"), Bytes.toBytes("X5"),
				Bytes.toBytes(values.get(1).toString()));
		put.add(Bytes.toBytes("Area"), Bytes.toBytes("X6"),
				Bytes.toBytes(values.get(2).toString()));
		put.add(Bytes.toBytes("Area"), Bytes.toBytes("Y1"),
				Bytes.toBytes(values.get(3).toString()));
		put.add(Bytes.toBytes("Area"), Bytes.toBytes("Y2"),
				Bytes.toBytes(values.get(4).toString()));
		put.add(Bytes.toBytes("Property"), Bytes.toBytes("X2"),
				Bytes.toBytes(values.get(5).toString()));
		put.add(Bytes.toBytes("Property"), Bytes.toBytes("X3"),
				Bytes.toBytes(values.get(6).toString()));
		put.add(Bytes.toBytes("Property"), Bytes.toBytes("X4"),
				Bytes.toBytes(values.get(7).toString()));
		put.add(Bytes.toBytes("Property"), Bytes.toBytes("X7"),
				Bytes.toBytes(values.get(8).toString()));
		put.add(Bytes.toBytes("Property"), Bytes.toBytes("X8"),
				Bytes.toBytes(values.get(9).toString()));

	}

	public static void addRecord(HTable table, String rowKey, String family,
			String qualifier, String value) throws IOException {
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),
				Bytes.toBytes(value));
		table.put(put);
		// System.out.println("Insert to the " + rowKey +
		// " record success for value - " + value);
	}

	public static Double distance(List<Double> values1, KeyValue[] values2) {
		if (values1.size() != values2.length) {
			return Double.MAX_VALUE;
		}

		/*
		 * System.out.println(
		 * "*******************Entered Distace FROM THE REDUCER*****************"
		 * ); System.out.println("Length of Values 1 - "+ values1.size());
		 * System.out.println("Length of Values 2 - "+ values2.length);
		 */

		/***
		 * Calculate Euclidian distance
		 ***/

		int length = values1.size();
		Double sumVals = 0D;
		for (int i = 0; i < length; i++) {
			Double value2 = new Double(new String(values2[i].getValue()));
			sumVals += Math.pow(values1.get(i) - value2, 2);
		}
		Double resturnResult = Math.sqrt(sumVals / length);
		// System.out.println("This is what I return to the reducer!" + result);
		return resturnResult;
	}

	public static Double distance(KeyValue[] values1, KeyValue[] values2) {
		if (values1.length != values2.length) {
			return Double.MAX_VALUE;
		}
		int length = values1.length;
		Double sumVals = 0D;
		for (int i = 0; i < length; i++) {
			// System.out.println(new String (values1[i].getValue()));
			// System.out.println(new String (values2[i].getValue()));
			Double value1 = new Double(new String(values1[i].getValue()));
			Double value2 = new Double(new String(values2[i].getValue()));
			sumVals += Math.pow(value1 - value2, 2.0);
		}
		Double resturnResult = Math.sqrt(sumVals / length);
		return resturnResult;
	}

	public static Map<String, KeyValue[]> loadFromHTable(String tableName)
			throws IOException {
		Map<String, KeyValue[]> centers = new HashMap<String, KeyValue[]>();
		Configuration config = HBaseConfiguration.create();
		HTable currentCenter = new HTable(config, tableName);
		Scan s = new Scan();
		ResultScanner scanner = currentCenter.getScanner(s);

		for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
			String key = new String(rr.getRow());
			KeyValue[] values = rr.raw();
			centers.put(key, values);
		}
		currentCenter.close();
		return centers;
	}

	public static String findNearestCenter(Map<String, KeyValue[]> centers,
			KeyValue[] point) {
		String neastCenterId = "";
		Double distance = Double.MAX_VALUE;
		// int i =0;
		for (Entry<String, KeyValue[]> center : centers.entrySet()) {
			// System.out.println("NOW CALCULATING DISTANCE WRT " +
			// center.getKey());

			/*
			 * System.out.println(); System.out.print("Row " + new String
			 * (point[i].getRow())); System.out.print(" Family " + new String
			 * (point[i].getFamily())); System.out.print(" Qualifier " + new
			 * String (point[i].getQualifier())); System.out.print(" Value " +
			 * new String (point[i].getValue())); System.out.println();
			 */

			Double dis = distance(center.getValue(), point);
			//System.out.println("Distance == " + dis);
			if (dis <= distance) {
				//System.out.println("Current distance lesser than previous for - "+ center.getKey());
				distance = dis;
				neastCenterId = center.getKey();
			}
			// i++;
		}
		// System.out.println("Sending the nearest ID " + neastCenterId);
		return neastCenterId;
	}

	public static List<Double> findAverage(Iterable<Result> results) {
		int number = 0;
		double[] centerValues = new double[10];
		for (Result result : results) {
			number++;
			KeyValue[] values = result.raw();
			for (int i = 0; i < values.length; i++) {
				centerValues[i] += new Double(new String(values[i].getValue()));
			}
		}

		for (int i = 0; i < centerValues.length; i++) {
			centerValues[i] = centerValues[i] / number;
		}

		List<Double> centerval = new ArrayList<Double>();
		for (double n : centerValues) {
			centerval.add(n);
		}
		return centerval;
	}
}
