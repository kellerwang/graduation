package example.hdfs;

import java.io.IOException;
import java.net.URI;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.DoubleWritable;
public class MapFileWriter {
	public static void main(String[] args) throws IOException {
		String uri = "sequence";
		// String uri = "try";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		LongWritable key = new LongWritable();
		MapWritable value = new MapWritable();
		MapFile.Writer writer = null;
		writer = new MapFile.Writer(conf, fs, uri, key.getClass(),
				value.getClass());
		for (int i = 0; i < 100; i++) {
			key.set(i + 1);
			for (int j = 0; j < 10; j++) {
				DoubleWritable temp = new DoubleWritable(0.1);
				System.out.println(temp);
				value.put(new LongWritable(j), temp);
			}
			for(Entry en : value.entrySet()){
				System.out.println(en.getValue());
			}
			System.out.println();
			writer.append(key, value);
		}
		IOUtils.closeStream(writer);
	}
}
