package org.apache.pig.impl.storm.state;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import backtype.storm.utils.WritableUtils;

public class CombineTupleWritable implements Writable {
	private List<Writable> values;
	
	public CombineTupleWritable() {
		
	}

	public CombineTupleWritable(Writable[] vals) {
		values = Arrays.asList(vals);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, values.size());
		for (int i = 0; i < values.size(); ++i) {
			Text.writeString(out, values.get(i).getClass().getName());
		}
		for (int i = 0; i < values.size(); ++i) {
			values.get(i).write(out);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int card = WritableUtils.readVInt(in);
		values = new ArrayList<Writable>(card);
		Class<? extends Writable>[] cls = new Class[card];
		try {
			for (int i = 0; i < card; ++i) {
				cls[i] = Class.forName(Text.readString(in)).asSubclass(Writable.class);
			}
			for (int i = 0; i < card; ++i) {
				values.add(i, cls[i].newInstance());
				values.get(i).readFields(in);
			}
		} catch (ClassNotFoundException e) {
			throw (IOException)new IOException("Failed tuple init").initCause(e);
		} catch (IllegalAccessException e) {
			throw (IOException)new IOException("Failed tuple init").initCause(e);
		} catch (InstantiationException e) {
			throw (IOException)new IOException("Failed tuple init").initCause(e);
		}
	}

	public Object get(int i) {
		return values.get(i);
	}
	
	public String toString() {
		return "CombineTupleWritable(" + values.toString() + ")";
	}
}
