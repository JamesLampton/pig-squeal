package org.apache.pig.impl.storm.oper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.storm.state.WindowBuffer;
import org.mortbay.util.ajax.JSON;

import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

public class TriWindowPersist implements ReducerAggregator<MapWritable> {
	
	Map<Integer, Long> windowSettings = new HashMap<Integer, Long>();
	
	public TriWindowPersist() {
		this(null);
	}
	
	public TriWindowPersist(String windowOptions) {
		// Parse the options if they're not null.
		if (windowOptions != null) {
			Map<String, Long> opts = (Map<String, Long>) JSON.parse(windowOptions);
			for (Entry<String, Long> ent : opts.entrySet()) {
				int i = Integer.parseInt(ent.getKey());
				windowSettings.put(i, ent.getValue());
			}
		}
	}

	static public List<NullableTuple> getTuples(MapWritable state) {
		// TODO: Handle windowed elements.
		
		List<NullableTuple> ret = new ArrayList<NullableTuple>();
		
		for (Entry<Writable, Writable> ent : state.entrySet()) {
			if (ent.getKey().getClass().isAssignableFrom(IntWritable.class)) {
				// This is a windowed element.
				WindowBuffer<NullableTuple> w = 
						(WindowBuffer<NullableTuple>) ent.getValue();
				ret.addAll(w.getWindow());
			} else {
				// This is a counted element.
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
		}
		
		return ret;
	}
	
	String dumpToString(MapWritable state) {
		StringBuilder sb = new StringBuilder();
		
		sb.append(this.getClass().getName()+ "[");
		
		for (Entry<Writable, Writable> ent : state.entrySet()) {
			if (ent.getKey().getClass().isAssignableFrom(NullableTuple.class)) {
				// It's a tuple -> count.
				sb.append(ent.getKey().toString());
				sb.append("->" + ent.getValue());
			} else {
				// It's a window.
				sb.append(ent.getKey().toString());
				sb.append("(idx)->" + ent.getValue());
			}
			
			sb.append(", ");
		}
		
		sb.append("]");
		
		return sb.toString();
	}

	@Override
	public MapWritable init() {
		MapWritable ret = new MapWritable();
		
		// Initialize any windows.
		for (Entry<Integer, Long> ent : windowSettings.entrySet()) {
			ret.put(new IntWritable(ent.getKey()), 
					new WindowBuffer<NullableTuple>(
							ent.getValue().intValue()));
		}
		
		return ret;
	}

	@Override
	public MapWritable reduce(MapWritable curr, TridentTuple tuple) {
		System.out.println("TriWindowPersist.reduce -- " + dumpToString(curr) + " " + tuple);
		
		// Pull the value.
		NullableTuple v = (NullableTuple) tuple.get(1);
		
		// Track the +/- stuff through.
		int c = tuple.getInteger(2);
		
		int idx = v.getIndex();
		Long ws = windowSettings.get(idx);
		if (ws != null) {
			IntWritable key_tmp = new IntWritable(idx);
			// Pull the window.
			WindowBuffer<NullableTuple> w = 
					(WindowBuffer<NullableTuple>) curr.get(key_tmp);
			
			if (c < 0) {
				// Remove the item for negative items.
				w.removeItem(v);
			} else {
				// Add it otherwise.
				w.push(v);
			}
		} else {
			IntWritable iw = (IntWritable) curr.get(v);
			if (iw == null) {
				iw = new IntWritable(c);
				curr.put(v, iw);
			} else {
				iw.set(iw.get() + c);
			}
		}				
		
		return curr;
	}
}
