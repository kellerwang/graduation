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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

import tongji.graduation.wangkaile.tagmodel.TagModel.TagModelMapper;
import tongji.graduation.wangkaile.tagmodel.TagModel.TagModelReducer;
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
			String strTemp[] = value.toString().split("\t");
			if (strTemp.length > 0) {
				targetFlag = strTemp[0];
				String strArray[] = strTemp[1].toString().split("#");
				if (strArray.length > 0) {
					String id = strArray[0];
					int nTemp = Integer.parseInt(strArray[1]);
					String valueTemp = strArray[2];
					if (targetFlag.equals("v1f")) {
						map_key.set(nTemp);
						map_value.set("v1f" + "#" + id + "#" + valueTemp);
						context.write(map_key, map_value);
					}
					if (targetFlag.equals("v2f")) {
						map_key.set(nTemp);
						map_value.set("v2f" + "#" + id + "#" + valueTemp);
						context.write(map_key, map_value);
					}
					if (targetFlag.equals("v3f")) {
						map_key.set(nTemp);
						map_value.set("v3f" + "#" + id + "#" + valueTemp);
						context.write(map_key, map_value);
					}
				}
			}
		}
	}

	public static class TagModelCombiner extends
			Reducer<IntWritable, Text, IntWritable, Text> {
		private Text reduce_value = new Text();

		private String v1f = "v1f";
		private String v2f = "v2f";
		private String v3f = "v3f";

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				String strArray[] = val.toString().split("#");
				String targetFlag = strArray[0];
				String id = strArray[1];
				String value = strArray[2];
				if (targetFlag.equals("v1f")) {
					v1f += "#" + id;
					v1f += "#" + value;
				}
				if (targetFlag.equals("v2f")) {
					v2f += "#" + id;
					v2f += "#" + value;
				}
				if (targetFlag.equals("v3f")) {
					v3f += "#" + id;
					v3f += "#" + value;
				}
			}
			if (!v1f.equals("v1f")) {
				reduce_value.set(v1f);
				context.write(key, reduce_value);
			}
			if (!v2f.equals("v2f")) {
				reduce_value.set(v2f);
				context.write(key, reduce_value);
			}
			if (!v3f.equals("v3f")) {
				reduce_value.set(v3f);
				context.write(key, reduce_value);
			}
		}
	}

	public static class TagModelReducer extends
			Reducer<IntWritable, Text, Text, Text> {
		private HashMap<Integer, Double> v1f = new HashMap<Integer, Double>();
		private HashMap<Integer, Double> v2f = new HashMap<Integer, Double>();
		private HashMap<Integer, Double> v3f = new HashMap<Integer, Double>();
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
						int id = Integer.parseInt(strArray[i]);
						double temp = Double.parseDouble(strArray[i + 1]);
						v1f.put(id, temp);
					}
				}
				if (targetFlag.equals("v2f")) {
					for (int i = 1; i < strArray.length; i = i + 2) {
						int id = Integer.parseInt(strArray[i]);
						double temp = Double.parseDouble(strArray[i + 1]);
						v2f.put(id, temp);
					}
				}
				if (targetFlag.equals("v3f")) {
					for (int i = 1; i < strArray.length; i = i + 2) {
						int id = Integer.parseInt(strArray[i]);
						double temp = Double.parseDouble(strArray[i + 1]);
						v3f.put(id, temp);
					}
				}
			}
			// read some parameter from hdfs
			// ---------------------------------------------------------------
			initializeW("W13", W13);
			initializeW("W23", W23);
			initializeSumW("sumW13", sumW13);
			initializeSumW("sumW23", sumW23);
			initializeSumW("sumW31", sumW31);
			initializeSumW("sumW32", sumW32);
			// compute v1Count v2Count v3Count
			// ---------------------------------------------------------------
			int v1Count = W13.size();
			int v2Count = W23.size();
			Hashtable<Integer, Boolean> w3Dic = new Hashtable<Integer, Boolean>();
			for (Hashtable<Integer, Double> de : W13.values()) {
				for (int v3 : de.keySet()) {
					if (!w3Dic.containsKey(v3)) {
						w3Dic.put(v3, true);
					}
				}
			}
			for (Hashtable<Integer, Double> de : W23.values()) {
				for (int v3 : de.keySet()) {
					if (!w3Dic.containsKey(v3)) {
						w3Dic.put(v3, true);
					}
				}
			}
			int v3Count = w3Dic.size();
			// ----------------------------------------------------------------
			// the n is the classification id in the formula
			int n = key.get();
			//
			// ------------------------------------------------------------------
			HashMap<Integer, Double> v2f2 = new HashMap<Integer, Double>();
			HashMap<Integer, Double> v3f2 = new HashMap<Integer, Double>();
			// compute v2f
			// ----------------------------------------------------------------
			for (int j = 0; j < v2Count; j++) {
				double denominator = sumW23.get(j);
				double sum = 0;
				if (denominator != 0) {
					for (int k = 0; k < v3Count; k++) {
						if (W23.get(j).containsKey(k) && v3f.containsKey(k)) {
							sum += (W23.get(j).get(k) * v3f.get(k) / denominator);
						}
					}
					v2f2.put(j, sum);
				}
				else{
					v2f2.put(j, sum);
				}
			}
			// compute v3f
			// ----------------------------------------------------------------
			for (int j = 0; j < v3Count; j++) {
				if (!sumW31.containsKey(j) || !sumW32.containsKey(j)) {
					continue;
				}
				double denominator = sumW31.get(j) + sumW32.get(j);
				double sum = 0;
				if (denominator != 0) {
					for (int k1 = 0; k1 < v1Count; k1++) {
						if (W13.get(k1).containsKey(j)) {
							sum += W13.get(k1).get(j) * v1f.get(k1)
									/ denominator;
						}
					}
					for (int k2 = 0; k2 < v2Count; k2++) {
						if (W23.get(k2).contains(j)) {
							sum += W23.get(k2).get(j) * v2f.get(k2)
									/ denominator;
						}
					}
					v3f2.put(j, sum);
				}
				else{
					v3f2.put(j, sum);
				}
			}
			// write v1f
			// ----------------------------------------------------------------
			for (Entry<Integer, Double> en : v1f.entrySet()) {
				String type = "v1f";
				int id = en.getKey();
				double tempValue = en.getValue();
				context.write(new Text(type), new Text(id + "#" + n + "#"
						+ tempValue));
			}
			// -----------------------------------------------------------------
			// write v2f
			for (Entry<Integer, Double> en : v2f2.entrySet()) {
				String type = "v2f";
				int id = en.getKey();
				double tempValue = en.getValue();
				context.write(new Text(type), new Text(id + "#" + n + "#"
						+ tempValue));
			}
			// ------------------------------------------------------------------
			// write v3f
			for (Entry<Integer, Double> en : v3f2.entrySet()) {
				String type = "v3f";
				int id = en.getKey();
				double tempValue = en.getValue();
				context.write(new Text(type), new Text(id + "#" + n + "#"
						+ tempValue));
			}
		}

		public void initializeSumW(String type, Hashtable<Integer, Double> sumW)
				throws IOException {
			String uri = "hdfs/parameter/" + type;
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			Path path = new Path(uri);
			IntWritable keyTemp = new IntWritable();
			DoubleWritable valueTemp = new DoubleWritable();
			SequenceFile.Reader reader = null;
			reader = new SequenceFile.Reader(fs, path, conf);
			while (reader.next(keyTemp, valueTemp)) {
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
					IntWritable tagId = (IntWritable) en.getKey();
					int tagIntId = tagId.get();
					DoubleWritable temp = (DoubleWritable) en.getValue();
					double tempDouble = temp.get();
					tempHash.put(tagIntId, tempDouble);
				}
				W.put(id, tempHash);
			}
			reader.close();
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String input = "hdfs/input/input0";
		String output = "hdfs/input/input1";
		Job job = new Job(conf, "tagmodel");
		job.setJarByClass(TagModel.class);
		job.setMapperClass(TagModelMapper.class);
		job.setCombinerClass(TagModelCombiner.class);
		job.setReducerClass(TagModelReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setNumReduceTasks(8);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
