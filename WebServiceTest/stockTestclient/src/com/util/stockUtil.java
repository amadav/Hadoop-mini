package com.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;
import org.w3c.dom.Element;

import NET.webserviceX.www.StockQuoteSoapProxy;

public class stockUtil {

	// Storage of the data
	private final static Map<String, List<Float>> hmapStore = new LinkedHashMap<String, List<Float>>();
	private static Map<String, String> stockList = new LinkedHashMap<String, String>();
	
	public void getStockSymbols() throws IOException {
		String fileName = "stockSymbols.txt";
		String workingDirectory = System.getProperty("user.dir");
		String absoluteFilePath = "";
		absoluteFilePath = workingDirectory + File.separator + "src"
				+ File.separator + fileName;

		File file = new File(absoluteFilePath);

		if (!file.exists()) {
			System.out.println("Stock List File Not Found!");
			System.err.println("Error!");
		}

		FileReader fileRead = new FileReader(file);
		BufferedReader buff = new BufferedReader(fileRead);
		String symbol;
		Map<String , String> stockMap = new HashMap<String, String>();
		while ((symbol = buff.readLine()) != null) {
			// stockSymbols.add(symbol);
			String[] sp = symbol.split(","); //0 = name, 1==symbol
			stock sto = new stock();
			/*sto.name = sp[0];
			sto.symbol = sp[1];
			stockL.add(sto);*/
			if(!stockMap.containsKey(sp[1].trim()))
			{
				stockMap.put(sp[1].trim(), sp[0].trim());
			}
		}
		buff.close();
		stockUtil.stockList = stockMap;
	}

	public void readStocksIntoFile() throws IOException {
		StockQuoteSoapProxy soap = new StockQuoteSoapProxy();
		Map<String, String> syms = stockUtil.stockList;
		String fileName = "fileRead.txt";
		String workingDirectory = System.getProperty("user.dir");
		String absoluteFilePath = "";
		absoluteFilePath = workingDirectory + File.separator + "src"
				+ File.separator + fileName;

		File filee = new File(absoluteFilePath);

		if (!filee.exists()) {
			filee.createNewFile();
		}

		FileWriter fWriter = new FileWriter(filee.getAbsoluteFile(), false);
		BufferedWriter bwuff = new BufferedWriter(fWriter);
		boolean firstFlag = true;
		String resulttemp;
		String result;
		System.out.println("Reading the stocks!");
		for (Map.Entry<String, String> symmap : syms.entrySet()) {
			String symbol = symmap.getKey();
			String response = soap.getQuote(symbol.trim());
			resulttemp = response.replaceAll("&", "&amp;");
			if (firstFlag) {
				result = resulttemp.replaceAll("</StockQuotes>", "");
				firstFlag = false;
			} else {
				String temp = resulttemp.replaceAll("</StockQuotes>", "");
				result = temp.replaceAll("<StockQuotes>", "");
			}
			//System.out.println(result);
			bwuff.write(result);
			bwuff.flush();

		}
		bwuff.write("</StockQuotes>");
		bwuff.close();
		System.out.println("----Written to File----");
		try {
			this.makeSense(filee);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void makeSense(File file) throws Exception {
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(file);
		doc.getDocumentElement().normalize();
		NodeList nList = doc.getElementsByTagName("Stock");
		System.out.println("----------------------------");

		for (int temp = 0; temp < nList.getLength(); temp++) {
			Node nNode = nList.item(temp);
			if (nNode.getNodeType() == Node.ELEMENT_NODE) {
				Element elem = (Element) nNode;

				stock st = new stock();
				st.last = Float.parseFloat(elem.getElementsByTagName("Last")
						.item(0).getTextContent());
				st.symbol = elem.getElementsByTagName("Symbol").item(0)
						.getTextContent();
				/*st.name = elem.getElementsByTagName("Name").item(0)
						.getTextContent();*/
				st.name = stockUtil.stockList.get(st.symbol);
				st.open = Float.parseFloat(elem.getElementsByTagName("Open")
						.item(0).getTextContent());

				if (!stockUtil.hmapStore.containsKey(st.symbol)) {
					List<Float> newList = new ArrayList<Float>();
					newList.add(st.last);
					stockUtil.hmapStore.put(st.symbol, newList);
				} else {
					List<Float> getList = stockUtil.hmapStore.get(st.symbol);
					getList.add(st.last);
					stockUtil.hmapStore.put(st.symbol, getList);
				}
			}
		}
		this.printHmap();
	}

	public void printHmap() throws IOException {
		
		for (Map.Entry<String, List<Float>> entry : stockUtil.hmapStore
				.entrySet()) {
			System.out.println("Symbol = " + entry.getKey());
			System.out.println("Values = " + entry.getValue());
		}

		String fileName = "abhishek_madav.txt";
		String workingDirectory = System.getProperty("user.dir");
		String absoluteFilePath = "";
		absoluteFilePath = workingDirectory + File.separator + fileName;

		File file = new File(absoluteFilePath);

		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else
			new PrintWriter(file).close();

		FileWriter fWriter = new FileWriter(file.getAbsoluteFile(), true);
		BufferedWriter bwuff = new BufferedWriter(fWriter);

		for (Map.Entry<String, List<Float>> entry : stockUtil.hmapStore
				.entrySet()) {
			List<Float> values = entry.getValue();
			bwuff.write("Symbol = " + entry.getKey());
			bwuff.newLine();
			bwuff.write("Name = " + stockUtil.stockList.get(entry.getKey()));
			bwuff.newLine();
			bwuff.write("Values --- ");
			bwuff.write(values.toString());
			bwuff.newLine();
			bwuff.write("****************");
			bwuff.newLine();
		}
		bwuff.close();

	}

	public void calfluctuations(Map<String, List<Float>> map) throws IOException {
		/*
		 * Steps: 1: Calculate mean for the stock values 2: Calculate standard
		 * deviation. 3: Calculate fluctuation.
		 */

		// Container for the fluctuations.
		Map<String, Float> mapSymToFluc = new HashMap<String, Float>();
		//List<stock> stockSyms = this.getStockSymbols();

		for (Map.Entry<String, String> syms : stockUtil.stockList.entrySet()) {
			String symbol = syms.getKey();
			List<Float> listOfValues = map.get(symbol.trim());
			float SD = this.getFluc(listOfValues);
			mapSymToFluc.put(syms.getKey(), SD);
		}
		this.writeIntoOutput(mapSymToFluc);
	}

	public float getFluc(List<Float> values) {
		double mean = this.getMean(values);
		double squareSum = 0;
		for (int i = 0; i < values.size(); i++) {
			squareSum += Math.pow(values.get(i) - mean, 2);
		}
		float standDev = (float) Math.sqrt((squareSum) / (values.size() - 1));
		return (float) (standDev / mean); // Fluctuation = SD/Mean in normalised
		// form.
	}

	public float getMean(List<Float> values) {
		float sum = 0;
		for (float f : values) {
			sum += f;
		}
		return (sum / values.size());
	}

	public void writeIntoOutput(Map<String, Float> result) throws IOException {
		String fileName = "result.txt";
		String workingDirectory = System.getProperty("user.dir");
		String absoluteFilePath = "";
		absoluteFilePath = workingDirectory + File.separator + fileName;

		File file = new File(absoluteFilePath);

		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else
			new PrintWriter(file).close();

		FileWriter fWriter = new FileWriter(file.getAbsoluteFile(), false);
		BufferedWriter bwuff = new BufferedWriter(fWriter);
		Map<String, Float> sortedResult = this.sortByComparator(result, false);
		boolean firstEntry = true;
		String symbolMax = null;
		float flucMax = 0;
		for (Map.Entry<String, Float> ent : sortedResult.entrySet()) {
			if (firstEntry) {
				firstEntry = false;
				symbolMax = ent.getKey();
				flucMax = ent.getValue().floatValue();
			}
			bwuff.write(ent.getKey());
			bwuff.write(" --- Fluctuation--- ");
			bwuff.write(ent.getValue().toString());
			bwuff.newLine();
			bwuff.flush();
		}

		// write the result
		bwuff.newLine();
		String resultString = "Maximum over: " + "[" + symbolMax
				+ "]" + " ==> " + stockUtil.stockList.get(symbolMax) + " with fluctuation " + flucMax;
		bwuff.write(resultString);
		bwuff.flush();
		bwuff.close();
		System.out.println("Written into the result file.");
	}

	private Map<String, Float> sortByComparator(Map<String, Float> unsortMap,
			final boolean order) {

		List<Entry<String, Float>> list = new LinkedList<Entry<String, Float>>(
				unsortMap.entrySet());

		// Sorting the list based on values
		Collections.sort(list, new Comparator<Entry<String, Float>>() {
			public int compare(Entry<String, Float> o1, Entry<String, Float> o2) {
				if (order) {
					return o1.getValue().compareTo(o2.getValue());
				} else {
					return o2.getValue().compareTo(o1.getValue());

				}
			}
		});

		// Maintaining insertion order with the help of LinkedList
		Map<String, Float> sortedMap = new LinkedHashMap<String, Float>();
		for (Entry<String, Float> entry : list) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}

	public static void main(String[] arg) throws InterruptedException,
	IOException {
		stockUtil st = new stockUtil();
		st.getStockSymbols();
		try {
			int value = 143; // iterations.
			for (int i = 0; i < value; i++) {
				st.readStocksIntoFile();
				Thread.sleep(600000); // Sleep for 10 minutes...600000
			}
			// Calculate Fluctuations
			st.calfluctuations(stockUtil.hmapStore);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
