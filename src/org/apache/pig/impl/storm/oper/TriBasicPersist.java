package org.apache.pig.impl.storm.oper;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.pig.data.InternalMap;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.util.ObjectSerializer;

import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

public class TriBasicPersist implements ReducerAggregator {
		
	@Override
	public MapWritable init() {
		return new MapWritable();
	}

	@Override
	public MapWritable reduce(Object curr, TridentTuple tuple) {
		MapWritable state = (MapWritable) curr;
		if (state == null) {
			state = init();
		}
		
		NullableTuple values = (NullableTuple) tuple.get(1);
		
		// Serialize the tuple to a string to be used as the key into the state.
		try {
			IntWritable iw = (IntWritable) state.get(values);
			if (iw == null) {
				iw = new IntWritable(1);
				state.put(values, iw);
			} else {
				iw.set(iw.get() + 1);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
		return state;
	}

	static public List<NullableTuple> getTuples(Object state_o) {
		List<NullableTuple> ret = new ArrayList<NullableTuple>();
		
		MapWritable state = (MapWritable) state_o;
		for (Entry<Writable, Writable> ent : state.entrySet()) {
			int c = ((IntWritable) ent.getValue()).get();
			NullableTuple v = (NullableTuple) ent.getKey();
			for (int i = 0; i < c; i++) {
				ret.add(v);
			}
		}
		
		return ret;
	}
}
