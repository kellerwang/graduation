package example.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriteFile {
	public static void main(String[] args) throws IOException {
		String uri = "hdfs/input/input0";
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(configuration);
		FSDataOutputStream out = null;
		out = fs.create(new Path(uri));
		out.writeBytes("");
		out.flush();
		out.close();
	}
}
