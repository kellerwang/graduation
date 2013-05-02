package tongji.graduation.wangkaile.tagmodel;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TagModel {
	// 
	private static int v1Count = 0;
	private static int v2Count = 0;
	private static int v3Count = 0;

	public static class TagModelMapper extends Mapper<Object, Text, Text, Text> {
		private Text map_key = new Text();
		private Text map_value = new Text();

		private String targetFlag;

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String strArray[] = value.toString().split("#");
			if (strArray.length > 0) {
				targetFlag = strArray[0];
				if (targetFlag.equals("v1f")) {
					String t = strArray[1];
					String n = strArray[2];
					String tn = strArray[3];
					// for v3f2
					for (int j = 0; j < v3Count; j++) {
						map_key.set(String.valueOf(j) + "#" + n);
						map_value.set("v1f" + "#" + t + "#" + tn);
						context.write(map_key, map_value);
					}
				}
				if (targetFlag.equals("v2f")) {
					String t = strArray[1];
					String n = strArray[2];
					String tn = strArray[3];
					// for v3f2
					for (int j = 0; j < v3Count; j++) {
						map_key.set(String.valueOf(j) + "#" + n);
						map_value.set("v2f" + "#" + t + "#" + tn);
						context.write(map_key, map_value);
					}
				}
				if (targetFlag.equals("v3f")) {
					String t = strArray[1];
					String n = strArray[2];
					String tn = strArray[3];
					// for v2f2
					for (int j = 0; j < v2Count; j++) {
						map_key.set(String.valueOf(j) + "#" + n);
						map_value.set("v3f" + "#" + t + "#" + tn);
						context.write(map_key, map_value);
					}
				}
				if (targetFlag.equals("W13")) {
					String t = strArray[1];
					String j = strArray[2];
					String tj = strArray[3];
					// for v3f2
					for (int n = 0; n < 8; n++) {
						map_key.set(j + "#" + String.valueOf(n));
						map_value.set("W13" + "#" + t + "#" + tj);
						context.write(map_key, map_value);
					}
				}
				if (targetFlag.equals("W23")) {
					String j1 = strArray[1];
					String t1 = strArray[2];
					String jt = strArray[3];
					// for v2f2
					for (int n = 0; n < 8; n++) {
						map_key.set(j1 + "#" + String.valueOf(n));
						map_value.set("W23v2f" + "#" + t1 + "#" + jt);
						context.write(map_key, map_value);
					}
					String t2 = strArray[1];
					String j2 = strArray[2];
					String tj = strArray[3];
					// for v3f2
					for (int n = 0; n < 8; n++) {
						map_key.set(j2 + "#" + String.valueOf(n));
						map_value.set("W23v3f" + "#" + t2 + "#" + tj);
						context.write(map_key, map_value);
					}
				}
				if (targetFlag.equals("sumW13")) {
					// 
				}
				if (targetFlag.equals("sumW23")) {
					String j = strArray[1];
					String v = strArray[2];
					for (int n = 0; n < 8; n++) {
						map_key.set(j + "#" + String.valueOf(n));
						map_value.set("sumW23" + "#" + v);
						context.write(map_key, map_value);
					}
				}
				if (targetFlag.equals("sumW31")) {
					String j = strArray[1];
					String v = strArray[2];
					for (int n = 0; n < 8; n++) {
						map_key.set(j + "#" + String.valueOf(n));
						map_value.set("sumW31" + "#" + v);
						context.write(map_key, map_value);
					}
				}
				if (targetFlag.equals("sumW32")) {
					String j = strArray[1];
					String v = strArray[2];
					for (int n = 0; n < 8; n++) {
						map_key.set(j + "#" + String.valueOf(n));
						map_value.set("sumW32" + "#" + v);
						context.write(map_key, map_value);
					}
				}
			}
		}
	}

	public static class TagModelReducer extends Reducer<Text, Text, Text, Text> {
		private Text reduce_key = new Text();
		private Text reduce_value = new Text();
		private String targetFlag;

		// to compute v2f
		private int W23v2f_jk[] = new int[v3Count];
		private int v3f_kn[] = new int[v3Count];

		// to compute v3f
		private int W13_k1j[] = new int[v1Count];
		private int v1f_k1n[] = new int[v1Count];

		private int W23v3f_k2j[] = new int[v2Count];
		private int v2f_k2n[] = new int[v2Count];

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// to compute v2f
			int v2fSum = 0;
			int sumW23_j = 0;
			// to compute v3f
			int v3fSum = 0;
			int sumW31_j = 0;
			int sumW32_j = 0;
			for (Text val : values) {
				String strArray[] = val.toString().split("#");
				targetFlag = strArray[0];
				if (targetFlag.equals("W23v2f")) {
					int k = Integer.parseInt(strArray[1]);
					W23v2f_jk[k] = Integer.parseInt(strArray[2]);
				}
				if (targetFlag.equals("v3f")) {
					int k = Integer.parseInt(strArray[1]);
					v3f_kn[k] = Integer.parseInt(strArray[2]);
				}
				if (targetFlag.equals("sumW23")) {
					sumW23_j = Integer.parseInt(strArray[1]);
				}
				if (targetFlag.equals("W13")) {
					int k1 = Integer.parseInt(strArray[1]);
					W13_k1j[k1] = Integer.parseInt(strArray[2]);
				}
				if (targetFlag.equals("v1f")) {
					int k1 = Integer.parseInt(strArray[1]);
					v1f_k1n[k1] = Integer.parseInt(strArray[2]);
				}
				if (targetFlag.equals("W23v3f")) {
					int k2 = Integer.parseInt(strArray[1]);
					W23v3f_k2j[k2] = Integer.parseInt(strArray[2]);
				}
				if (targetFlag.equals("v2f")) {
					int k2 = Integer.parseInt(strArray[1]);
					v2f_k2n[k2] = Integer.parseInt(strArray[2]);
				}
				if (targetFlag.equals("sumW31")) {
					sumW31_j = Integer.parseInt(strArray[1]);
				}
				if (targetFlag.equals("sumW32")) {
					sumW32_j = Integer.parseInt(strArray[1]);
				}
			}
			// compute v2f2
			if (sumW23_j != 0) {
				for (int k = 0; k < v3Count; k++) {
					v2fSum += W23v2f_jk[k] * v3f_kn[k] / sumW23_j;
				}
			}
			// compute v3f2
			int sum_v3f = sumW31_j + sumW32_j;
			if (sum_v3f != 0) {
				for (int k1 = 0; k1 < v1Count; k1++) {
					v3fSum += W13_k1j[k1] * v1f_k1n[k1] / sum_v3f;
				}
				for (int k2 = 0; k2 < v2Count; k2++) {
					v3fSum += W23v3f_k2j[k2] * v2f_k2n[k2] / sum_v3f;
				}
			}
			// get new key and value for v2f
			reduce_key.set("v2f" + "#" + key.toString());
			reduce_value.set(String.valueOf(v2fSum));
			context.write(reduce_key, reduce_value);
			// get new key and value for v2f
			reduce_key.set("v3f" + "#" + key.toString());
			reduce_value.set(String.valueOf(v3fSum));
			context.write(reduce_key, reduce_value);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: tagmodel <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "tagmodel");
		job.setJarByClass(TagModel.class);
		job.setMapperClass(TagModelMapper.class);
		job.setReducerClass(TagModelReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
