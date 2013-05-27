package tongji.graduation.wangkaile.tagmodel.accuracy;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
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
import org.apache.hadoop.util.LineReader;

public class Accuracy {
	
	public static class AccuracyMapper extends
			Mapper<Object, Text, IntWritable, Text> {
		private IntWritable map_key = new IntWritable();
		private Text map_value = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String strTemp[] = value.toString().split("\t");
			if (strTemp.length > 0) {
				String targetFlag = strTemp[0];
				if (targetFlag.equals("v2f")) {
					String strArray[] = strTemp[1].toString().split("#");
					if (strArray.length > 0) {
						int id = Integer.parseInt(strArray[0]);
						int nTemp = Integer.parseInt(strArray[1]);
						String valueTemp = strArray[2];
						map_key.set(id);
						map_value.set(nTemp + "#" + valueTemp);
						context.write(map_key, map_value);
					}
				}
			}
		}
	}

	public static class AccuracyReducer extends
			Reducer<IntWritable, Text, IntWritable, IntWritable> {
		private IntWritable reduce_value = new IntWritable();

		static private Hashtable<Integer, Integer> trueV2ClassDic = new Hashtable<Integer, Integer>();

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
						trueV2ClassDic = (Hashtable<Integer, Integer>) in
								.readObject();
					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			HashMap<Integer, Double> tempList = new HashMap<Integer, Double>();
			for (Text val : values) {
				String strArray[] = val.toString().split("#");
				int nTemp = Integer.parseInt(strArray[0]);
				double valueTemp = Double.parseDouble(strArray[1]);
				tempList.put(nTemp, valueTemp);
			}
			double sum = 0;
			for (int i = 0; i < 8; i++) {
				sum += tempList.get(i);
			}
			int maxlabel = 0;
			if (sum != 0) {
				for (int i = 0; i < 8; i++) {
					double temp = tempList.get(i);
					temp = temp / sum;
					tempList.put(i, temp);
				}
				double maxf = -1;
				for (int i = 0; i < 8; i++) {
					if (tempList.get(i) > maxf) {
						maxlabel = i;
						maxf = tempList.get(i);
					}
				}
			}
			// System.out.println("trueV2ClassDic.get(key.get(): " +
			// trueV2ClassDic.get(key.get()));
			// System.out.println("maxlabel: " + maxlabel);
			if (trueV2ClassDic.get(key.get()) == maxlabel) {
				// accuracyNum++;
				// System.out.println("accuracyNum: " + accuracyNum);
				reduce_value.set(maxlabel);
				context.write(key, reduce_value);
			}
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException, URISyntaxException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		DistributedCache.addCacheFile(new URI(
				"/usr/wangkaile/tagmodel/parameters/trueV2ClassDic"), conf);
		String input = "/usr/wangkaile/tagmodel/input1/input0";
		String output = "/usr/wangkaile/tagmodel/output";
		Job job = new Job(conf, "accracy");
		job.setJarByClass(Accuracy.class);
		job.setMapperClass(AccuracyMapper.class);
		job.setReducerClass(AccuracyReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileSystem.get(job.getConfiguration()).delete(new Path(output), true);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setNumReduceTasks(1);
		job.waitForCompletion(true);
		String uri = "/usr/wangkaile/tagmodel/output/part-r-00000";
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = null;
		in = fs.open(new Path(uri));
		LineReader lr = new LineReader(in, conf);
		Text line = new Text();
		int totalLines = 0;
		while (lr.readLine(line) > 0) {
			 totalLines++;
		}
		in.close();
		lr.close();
		System.out.println("accuracyNum: " + totalLines);
	}

}
