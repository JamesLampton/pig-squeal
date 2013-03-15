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
import storm.trident.tuple.TridentTuple;

public class TriWindowCombinePersist implements CombinerAggregator<MapWritable> {
	
	Map<Integer, Long> windowSettings = new HashMap<Integer, Long>();
	
	public TriWindowCombinePersist() {
		this(null);
	}
	
	public TriWindowCombinePersist(String windowOptions) {
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
	
	void addTuple(MapWritable s, NullableTuple t, int c) {
		int idx = t.getIndex();
		Long ws = windowSettings.get(idx);
		if (ws != null) {
			IntWritable key_tmp = new IntWritable(idx);

			// Pull the window.
			WindowBuffer<NullableTuple> w = 
					(WindowBuffer<NullableTuple>) s.get(key_tmp);
			
			/*
			 * FIXME: If we get the negative before the positive, this won't work.
			 * The proper way to do this would be to count the removes in window
			 * state so we can ignore adds when the matching positive values come
			 * in. 
			 */
			if (c < 0) {
				// Remove the item for negative items.
				w.removeItem(t);
			} else {
				// Add it otherwise.
				w.push(t);
			}
		} else {
			// This is not a windowed element, just add like BASEPERSIST.
			IntWritable iw = (IntWritable) s.get(t);
			if (iw == null) {
				iw = new IntWritable(c);
				s.put(t, iw);
			} else {
				iw.set(iw.get() + c);
			}
		}
	}
	
	@Override
	public MapWritable init(TridentTuple tuple) {
		MapWritable ret = zero();
		NullableTuple values = (NullableTuple) tuple.get(1);
		
		// Track the +/- stuff through.
		addTuple(ret, values, tuple.getInteger(2));
		return ret;
	}
	
	String dumpToString(MapWritable state) {
		StringBuilder sb = new StringBuilder();
		
		sb.append(this.getClass().getName()+"[");
		
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
	public MapWritable combine(MapWritable val1, MapWritable val2) {
//		System.out.println("TriBasicPersist.combine -- ");
//		System.out.println("val1 -- " + dumpToString(val1));
//		System.out.println("val2 -- " + dumpToString(val2));

		// This is written with the assumption that val1 comes from and to the 
		// storage mechanism while val2 comes from init...
		if (val1 == null) {
			val1 = zero();
		}
		
		// We're going to merge into val1
		if (val2 != null) {
			for (Entry<Writable, Writable> ent : val2.entrySet()) {
				// See if this is a windowed element.
				if (ent.getKey() instanceof IntWritable) {
					// Pull the window.
					WindowBuffer<NullableTuple> w = 
							(WindowBuffer<NullableTuple>) val1.get(ent.getKey());
					
					// Merge win2 in to w.
					WindowBuffer<NullableTuple> w2 = (WindowBuffer<NullableTuple>) ent.getValue(); 
					for (NullableTuple v : w2.getWindow()) {
						w.push(v);
					}
					
					continue;
				}
				
				// Otherwise it's a tuple, merge it.
				NullableTuple v = (NullableTuple) ent.getKey();
				int c = ((IntWritable)ent.getValue()).get();
				this.addTuple(val1, v, c);
			}
		}
		
		return val1;
	}

	@Override
	public MapWritable zero() {
		MapWritable ret = new MapWritable();
		
		// Initialize any windows.
		for (Entry<Integer, Long> ent : windowSettings.entrySet()) {
			ret.put(new IntWritable(ent.getKey()), 
					new WindowBuffer<NullableTuple>(
							ent.getValue().intValue()));
		}
		
		return ret;
	}
}
