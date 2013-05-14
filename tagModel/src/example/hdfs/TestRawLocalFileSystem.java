package example.hdfs;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class TestRawLocalFileSystem {
	private static HashMap<String, String> errorList = new HashMap<String, String>();
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String uri = "hdfs/distributedCache/errorWordList.txt";
		Configuration conf = new Configuration();
		
//		Path path = new Path(uri);
		Path localCacheFile = new Path(uri);
//		FileSystem fs = FileSystem.get(URI.create(localCacheFile.toString()), context.getConfiguration());
//		FileSystem fs = new RawLocalFileSystem();
		FileSystem fs =  FileSystem.getLocal(new Configuration());
		Path path = new Path(localCacheFile.toString());
		Text key = new Text();
		Text value = new Text();
		SequenceFile.Reader reader = null;
		reader=new SequenceFile.Reader(fs, path, conf);
		while(reader.next(key,value)){
			errorList.put(key.toString(), value.toString());
		}
	}

}
