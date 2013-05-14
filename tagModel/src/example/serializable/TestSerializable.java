package example.serializable;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestSerializable {

	/**
	 * @param args
	 * @throws IOException
	 * @throws FileNotFoundException
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws FileNotFoundException,
			IOException, ClassNotFoundException {
		// TODO Auto-generated method stub
		String uri = "serializable/objectFile.obj";
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(configuration);
		ObjectOutputStream out = new ObjectOutputStream(fs.create(new Path(uri)));
		Hashtable<Integer, Hashtable<Integer, Double>> W13 = new Hashtable<Integer, Hashtable<Integer, Double>>();
		for (int i = 0; i < 10; i++) {
			Hashtable<Integer, Double> temp = new Hashtable<Integer, Double>();
			for (int j = 0; j < 10; j++) {
				temp.put(j, 0.1);
			}
			W13.put(i, temp);
		}
		out.writeObject(W13);
		out.close();
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(
				"serializable/objectFile.obj"));
		Hashtable<Integer, Hashtable<Integer, Double>> W23 = (Hashtable<Integer, Hashtable<Integer, Double>>) in.readObject();
		for (int i = 0; i < 10; i++) {
			
			for (int j = 0; j < 10; j++) {
				double temp = W23.get(i).get(j);
				System.out.println(i + " " + j + " " + temp);
			}
			
		}
	}

}
