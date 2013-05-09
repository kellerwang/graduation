package tongji.graduation.wangkaile.tagmodel.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;

public class SameKeyList implements Writable {
	private int type;
	private HashMap<Long, Double> list;

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public HashMap<Long, Double> getList() {
		return list;
	}

	public SameKeyList() {
		list = new HashMap<Long, Double>();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.type = in.readInt();
		int size = in.readInt();
		for(int i = 0; i < size; i++){
			long key = in.readLong();
			double value = in.readDouble();
			this.list.put(key, value);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(this.type);
		out.writeInt(this.list.size());
		for(Entry<Long, Double> en : list.entrySet()){
			long key = en.getKey();
			double value = en.getValue();
			out.writeLong(key);
			out.writeDouble(value);
		}
	}

}
