package example.hdfs;

import java.io.IOException;
import java.net.URI;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class WriteSequenceFile {
	public static void main(String[] args) throws IOException {
		String uri = "hdfs/distributedCache/errorWordList.txt";
		Configuration conf = new Configuration();
		FileSystem fs;
		fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		SequenceFile.Writer writer = null;
		writer = new SequenceFile.Writer(fs, conf, path, Text.class, Text.class);
		Text key = new Text();
		Text value = new Text();
		key.set("kola");
		value.set("cola");
		writer.append(key, value);
		key.set("cala");
		value.set("cola");
		writer.append(key, value);
		IOUtils.closeStream(writer);
	}
}
