package tongji.graduation.wangkaile.tagmodel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

import tongji.graduation.wangkaile.tagmodel.writable.SameKeyList;

public class NewTagModel {
	private String uriW13;
	private String uriW23;
	private String uriSumW13;
	private String uriSumW23;
	private String uriSumW31;
	private String uriSumW32;

	public static class TagModelMapper extends
			Mapper<Object, Text, IntWritable, Text> {
		private IntWritable map_key = new IntWritable();
		private Text map_value = new Text();
		// determine the type of the input
		private String targetFlag;

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String strArray[] = value.toString().split("#");
			if (strArray.length > 0) {
				targetFlag = strArray[0];
				String id = strArray[1];
				if (targetFlag.equals("v1f")) {
					for (int n = 0; n < 8; n++) {
						map_key.set(n);
						map_value.set("v1f" + "#" + id + "#" + strArray[2 + n]);
						context.write(map_key, map_value);
					}
				}
				if (targetFlag.equals("v2f")) {
					for (int n = 0; n < 8; n++) {
						map_key.set(n);
						map_value.set("v2f" + "#" + id + "#" + strArray[2 + n]);
						context.write(map_key, map_value);
					}
				}
				if (targetFlag.equals("v3f")) {
					for (int n = 0; n < 8; n++) {
						map_key.set(n);
						map_value.set("v3f" + "#" + id + "#" + strArray[2 + n]);
						context.write(map_key, map_value);
					}
				}
			}
		}
	}

	public static class TagModelCombiner extends
			Reducer<IntWritable, Text, IntWritable, Text> {
		private Text reduce_value = new Text();

		private String v1f = "v1f" + "#";
		private String v2f = "v2f" + "#";
		private String v3f = "v3f" + "#";

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				String strArray[] = val.toString().split("#");
				String targetFlag = strArray[0];
				String id = strArray[1];
				String value = strArray[2];
				if (targetFlag.equals("v1f")) {
					v1f += id;
					v1f += value;
				}
				if (targetFlag.equals("v2f")) {
					v2f += id;
					v2f += value;
				}
				if (targetFlag.equals("v3f")) {
					v3f += id;
					v3f += value;
				}
			}
			reduce_value.set(v1f);
			context.write(key, reduce_value);
			reduce_value.set(v2f);
			context.write(key, reduce_value);
			reduce_value.set(v3f);
			context.write(key, reduce_value);
		}
	}

	public static class TagModelReducer extends
			Reducer<IntWritable, Text, Text, Text> {
		private HashMap<Long, Double> v1f = new HashMap<Long, Double>();
		private HashMap<Long, Double> v2f = new HashMap<Long, Double>();
		private HashMap<Long, Double> v3f = new HashMap<Long, Double>();
		private Hashtable<Integer, Hashtable<Integer, Double>> W13 = new Hashtable<Integer, Hashtable<Integer, Double>>();
		private Hashtable<Integer, Hashtable<Integer, Double>> W23 = new Hashtable<Integer, Hashtable<Integer, Double>>();
		private Hashtable<Integer, Double> sumW13 = new Hashtable<Integer, Double>();
		private Hashtable<Integer, Double> sumW23 = new Hashtable<Integer, Double>();
		private Hashtable<Integer, Double> sumW31 = new Hashtable<Integer, Double>();
		private Hashtable<Integer, Double> sumW32 = new Hashtable<Integer, Double>();

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			// deal with the input of v1 v2 v3
			// -------------------------------------------------------------
			for (Text val : values) {
				String strArray[] = val.toString().split("#");
				String targetFlag = strArray[0];
				if (targetFlag.equals("v1f")) {
					for (int i = 1; i < strArray.length; i = i + 2) {
						long id = Long.parseLong(strArray[i]);
						double temp = Double.parseDouble(strArray[i + 1]);
						v1f.put(id, temp);
					}
				}
				if (targetFlag.equals("v2f")) {
					for (int i = 1; i < strArray.length; i = i + 2) {
						long id = Long.parseLong(strArray[i]);
						double temp = Double.parseDouble(strArray[i + 1]);
						v2f.put(id, temp);
					}
				}
				if (targetFlag.equals("v3f")) {
					for (int i = 1; i < strArray.length; i = i + 2) {
						long id = Long.parseLong(strArray[i]);
						double temp = Double.parseDouble(strArray[i + 1]);
						v3f.put(id, temp);
					}
				}
			}

			initializeW("W13", W13);
			initializeW("W23", W23);
			initializeSumW("sumW13", sumW13);
			initializeSumW("sumW23", sumW23);
			initializeSumW("sumW31", sumW31);
			initializeSumW("sumW32", sumW32);
		}

		public void initializeSumW(String type, Hashtable<Integer, Double> sumW) throws IOException{
			String uri = "hdfs/parameter/" + type;
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			Path path = new Path(uri);
			IntWritable keyTemp = new IntWritable();
			DoubleWritable valueTemp = new DoubleWritable();
			SequenceFile.Reader reader = null;
			reader=new SequenceFile.Reader(fs, path, conf);
			while(reader.next(keyTemp,valueTemp)){  
				sumW.put(keyTemp.get(), valueTemp.get());
			}
			reader.close();
		}
		
		public void initializeW(String type,
				Hashtable<Integer, Hashtable<Integer, Double>> W)
				throws IOException {
			String uri = "hdfs/parameter/" + type;
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			Path path = new Path(uri);
			IntWritable keyTemp = new IntWritable();
			MapWritable valueTemp = new MapWritable();
			SequenceFile.Reader reader = null;
			reader = new SequenceFile.Reader(fs, path, conf);
			while (reader.next(keyTemp, valueTemp)) {
				int id = keyTemp.get();
				Hashtable<Integer, Double> tempHash = new Hashtable<Integer, Double>();
				for (Entry en : valueTemp.entrySet()) {
					int tagId = (int) en.getKey();
					double temp = (double) en.getValue();
					tempHash.put(tagId, temp);
				}
				W.put(id, tempHash);
			}
			reader.close();
		}
	}
}
