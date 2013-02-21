package org.apache.pig.impl.storm.oper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.pig.impl.io.NullableTuple;

import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

public class TriBasicPersist implements CombinerAggregator<MapWritable> {

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

	@Override
	public MapWritable init(TridentTuple tuple) {
		MapWritable ret = new MapWritable();
		NullableTuple values = (NullableTuple) tuple.get(1);
		ret.put(values, new IntWritable(1));
		return ret;
	}

	@Override
	public MapWritable combine(MapWritable val1, MapWritable val2) {
		
		// Going under the assumption that val1 came from the cache/store.
		
		// val2 are the new values.

		
		// We're going to merge into val1.
		if (val1 == null) {
			val1 = new MapWritable();
		}
		
		if (val2 != null) {
			for (Entry<Writable, Writable> ent : val2.entrySet()) {
				NullableTuple values = (NullableTuple) ent.getKey();
				int c = ((IntWritable) ent.getValue()).get();
				IntWritable iw = (IntWritable) val1.get(values);
				if (iw == null) {
					iw = new IntWritable(c);
					val1.put(values, iw);
				} else {
					iw.set(iw.get() + c);
				}
			}
		}
		
		return val1;
	}

	@Override
	public MapWritable zero() {
		return null;
	}
}
