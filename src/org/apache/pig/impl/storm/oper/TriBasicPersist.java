package org.apache.pig.impl.storm.oper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.pig.impl.io.NullableTuple;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class TriBasicPersist implements CombinerAggregator<MapWritable> {
	
	static public List<NullableTuple> getTuples(MapWritable state) {
		List<NullableTuple> ret = new ArrayList<NullableTuple>();
		
		for (Entry<Writable, Writable> ent : state.entrySet()) {
			int c = ((IntWritable) ent.getValue()).get();
			NullableTuple v = (NullableTuple) ent.getKey();
			// If c is negative then we may have seen the inverse tuple for 
			// a positive tuple we've yet to see.  This will silently consume
			// them.
			// FIXME: Or we have a terrible problem...
			for (int i = 0; i < c; i++) {
				ret.add(v);
			}
		}
		
		return ret;
	}

	@Override
	public MapWritable init(TridentTuple tuple) {
		MapWritable ret = zero();
		NullableTuple values = (NullableTuple) tuple.get(1);
		
		// Track the +/- stuff through.
		ret.put(values, new IntWritable(tuple.getInteger(2)));
		return ret;
	}

	@Override
	public MapWritable combine(MapWritable val1, MapWritable val2) {
		MapWritable ret = zero();
		
		if (val1 != null) {
			for (Entry<Writable, Writable> ent : val1.entrySet()) {
				ret.put(ent.getKey(), new IntWritable(((IntWritable) ent.getValue()).get()));
			}
		}
		
		// We're going to merge into val1.
		if (val2 != null) {
			for (Entry<Writable, Writable> ent : val2.entrySet()) {
				int c = ((IntWritable) ent.getValue()).get();
				IntWritable iw = (IntWritable) ret.get(ent.getKey());
				if (iw == null) {
					iw = new IntWritable(c);
					ret.put(ent.getKey(), iw);
				} else {
					iw.set(iw.get() + c);
				}
			}
		}
		
		return ret;
	}

	@Override
	public MapWritable zero() {
		return new MapWritable();
	}
}
