package tongji.graduation.wangkaile.tagmodel;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import tongji.graduation.wangkaile.tagmodel.NewTagModel.TagModelCombiner;
import tongji.graduation.wangkaile.tagmodel.NewTagModel.TagModelMapper;
import tongji.graduation.wangkaile.tagmodel.NewTagModel.TagModelReducer;

public class TagModel5 {
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

	public static class TagModelReducer extends
			Reducer<IntWritable, Text, Text, Text> {
		private HashMap<Integer, Double> v1f = new HashMap<Integer, Double>();
		private HashMap<Integer, Double> v2f = new HashMap<Integer, Double>();
		private HashMap<Integer, Double> v3f = new HashMap<Integer, Double>();
		static private Hashtable<Integer, Hashtable<Integer, Double>> W13;
		static private Hashtable<Integer, Hashtable<Integer, Double>> W23;
		static private Hashtable<Integer, Double> sumW13;
		static private Hashtable<Integer, Double> sumW23;
		static private Hashtable<Integer, Double> sumW31;
		static private Hashtable<Integer, Double> sumW32;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			Path[] localCacheFiles = DistributedCache
					.getLocalCacheFiles(context.getConfiguration());
			if (localCacheFiles != null) {
				for (int i = 0; i < localCacheFiles.length; i++) {
					Path localCacheFile = localCacheFiles[i];
					ObjectInputStream in = new ObjectInputStream(
							new FileInputStream(localCacheFile.toString()));
					try {
						W13 = (Hashtable<Integer, Hashtable<Integer, Double>>) in
								.readObject();
						W23 = (Hashtable<Integer, Hashtable<Integer, Double>>) in
								.readObject();
						sumW13 = (Hashtable<Integer, Double>) in.readObject();
						sumW23 = (Hashtable<Integer, Double>) in.readObject();
						sumW31 = (Hashtable<Integer, Double>) in.readObject();
						sumW32 = (Hashtable<Integer, Double>) in.readObject();
					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			// deal with the input of v1 v2 v3
			// -------------------------------------------------------------
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			System.out.println(df.format(new Date()));
			for (Text val : values) {
				String strArray[] = val.toString().split("#");
				String targetFlag = strArray[0];
				if (targetFlag.equals("v1f")) {
					int id = Integer.parseInt(strArray[1]);
					double temp = Double.parseDouble(strArray[2]);
					v1f.put(id, temp);
				}
				if (targetFlag.equals("v2f")) {
					int id = Integer.parseInt(strArray[1]);
					double temp = Double.parseDouble(strArray[2]);
					v2f.put(id, temp);
				}
				if (targetFlag.equals("v3f")) {
					int id = Integer.parseInt(strArray[1]);
					double temp = Double.parseDouble(strArray[2]);
					v3f.put(id, temp);
				}
			}
			System.out.println("after map's input: " + df.format(new Date()));
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
			System.out.println("after deal with parameter: "
					+ df.format(new Date()));
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
				} else {
					v2f2.put(j, sum);
				}
			}
			System.out.println("after compute v2: " + df.format(new Date()));
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
				} else {
					v3f2.put(j, sum);
				}
			}
			System.out.println("after compute v3: " + df.format(new Date()));
			// 
			// ----------------------------------------------------------------
			
			
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
			System.out.println("after write: " + df.format(new Date()));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		DistributedCache.addCacheFile(new URI(
				"/usr/wangkaile/tagmodel/parameters/parameter"), conf);
		conf.setInt("mapred.reduce.parallel.copies", 8);
		String input = "/usr/wangkaile/tagmodel/input1/input0";
		String output = "/usr/wangkaile/tagmodel/input1/input1";
		Job job = new Job(conf, "tagmodel");
		job.setJarByClass(TagModel.class);
		job.setMapperClass(TagModelMapper.class);
		job.setReducerClass(TagModelReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileSystem.get(job.getConfiguration()).delete(new Path(output), true);// 如果文件已存在删除
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setNumReduceTasks(8);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
