package example.hdfs;

import java.io.IOException;
import java.net.URI;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;

public class ReadFile {
	public static void main(String[] args) throws IOException {
		String uri = "hdfs/parameter/sumW13";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);
		IntWritable key = new IntWritable();
		DoubleWritable value = new DoubleWritable();
		SequenceFile.Reader reader = null;
		reader=new SequenceFile.Reader(fs, path, conf);
		while(reader.next(key,value)){  
		    System.out.println(key + " " + value);
		    
		}
		reader.close();
	}
}
