package org.apache.pig.impl.storm.oper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.storm.state.IPigIdxState;
import org.apache.pig.impl.storm.state.MapIdxWritable;
import org.apache.pig.impl.storm.state.WindowBuffer;
import org.apache.pig.impl.util.Pair;
import org.mortbay.util.ajax.JSON;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class TriWindowCombinePersist implements CombinerAggregator<MapIdxWritable> {
	
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

	static public List<NullableTuple> getTuples(MapIdxWritable state) {		
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
	
	void addTuple(MapIdxWritable s, NullableTuple t, int c) {
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
	public MapIdxWritable init(TridentTuple tuple) {
		MapIdxWritable ret = zero();
		NullableTuple values = (NullableTuple) tuple.get(1);
		
		// Track the +/- stuff through.
		addTuple(ret, values, tuple.getInteger(2));
		return ret;
	}
	
	String dumpToString(MapIdxWritable state) {
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
	
	void mergeValues(MapIdxWritable into, MapIdxWritable from) {
		for (Entry<Writable, Writable> ent : from.entrySet()) {
			// See if this is a windowed element.
			if (ent.getKey() instanceof IntWritable) {
				// Pull the window.
				WindowBuffer<NullableTuple> w = 
						(WindowBuffer<NullableTuple>) into.get(ent.getKey());
				
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
			this.addTuple(into, v, c);
		}
	}

	@Override
	public MapIdxWritable combine(MapIdxWritable val1, MapIdxWritable val2) {
//		System.out.println("TriWindowCombinePersist.combine -- ");
//		System.out.println("val1 -- " + dumpToString(val1));
//		System.out.println("val2 -- " + dumpToString(val2));
		
		MapIdxWritable ret = zero();
		
		// Merge the values
		mergeValues(ret, val1);
		mergeValues(ret, val2);
		
		return ret;
	}
	
	static public class WindowCombineState extends MapIdxWritable {
		private Map<Integer, Long> settings;
		
		// Override default write to track the settings in case
		// some of the elements age off.
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(settings.size());
			for (java.util.Map.Entry<Integer, Long> ent : settings.entrySet()) {
				out.writeInt(ent.getKey());
				out.writeLong(ent.getValue());
			}
			super.write(out);
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			int size = in.readInt();
			this.clear();
			Map<Integer, Long> in_set = new HashMap<Integer, Long>(size);
			for (int i = 0; i < size; i++) {
				int k = in.readInt();
				long v = in.readLong();
				in_set.put(k, v);
			}
			init(in_set);
			super.readFields(in);
		}

		public WindowCombineState() {
			// Needed for readFields.
		}
		
		public WindowCombineState(Map<Integer, Long> windowSettings) {
			init(windowSettings);
		}
		
		void init(Map<Integer, Long> windowSettings) {
			this.settings = windowSettings;
			
			// Initialize any windows.
			for (Entry<Integer, Long> ent : windowSettings.entrySet()) {
				put(new IntWritable(ent.getKey()), 
						new WindowBuffer<NullableTuple>(
								ent.getValue().intValue()));
			}
		}
		
		@Override
		public List<NullableTuple> getTuples(Text which) {
			return TriWindowCombinePersist.getTuples(this);
		}

		@Override
		public Pair<Writable, List<Writable>> separate(List<Integer[]> bins) {
			MapIdxWritable def = null;
			List<MapIdxWritable> ret = new ArrayList<MapIdxWritable>(bins.size());
			HashMap<Integer, Integer> idxMap = new HashMap<Integer, Integer>();
			
			for (Integer[] bin : bins) {
//				MapIdxWritable st = new WindowCombineState(settings);
				for (Integer i : bin) {
					idxMap.put(i, ret.size());
				}
				ret.add(null);
			}
			
			for (Entry<Writable, Writable> ent : entrySet()) {
				int idx;
				if (ent.getKey().getClass().isAssignableFrom(IntWritable.class)) {
					// This is a windowed element.
					idx = ((IntWritable)ent.getKey()).get(); 
				} else {
					NullableTuple v = (NullableTuple) ent.getKey();
					idx = v.getIndex();
				}

				// Look up the appropriate state.
				Integer mappedIdx = idxMap.get(idx);
				MapIdxWritable mapped; // = idxMap.get(idx);
				if (mappedIdx == null) {
					if (def == null) {
						def = new WindowCombineState(settings);
					}
					mapped = def;
				} else {
					mapped = ret.get(mappedIdx);
					if (mapped == null) {
						mapped = new WindowCombineState(settings);
						ret.set(mappedIdx, mapped);
					}
				}
				mapped.put(ent.getKey(), ent.getValue());
			}
			
			return new Pair(def, ret);
		}

		@Override
		public void merge(IPigIdxState other) {
			for (Entry<Writable, Writable> ent : ((WindowCombineState)other).entrySet()) {
				if (ent.getKey().getClass().isAssignableFrom(IntWritable.class)) {
					// This is a windowed element.
					WindowBuffer<NullableTuple> w = (WindowBuffer<NullableTuple>) ent.getValue();
					// The windows should have been partitioned, so if we have anything within
					// replace ours.
					if (w.size() > 0) {
						put(ent.getKey(), ent.getValue());
					}
				} else {
					// Not windowed, just put it.
					put(ent.getKey(), ent.getValue());
				}
			}
		}
	}
	
	@Override
	public MapIdxWritable zero() {
		MapIdxWritable ret = new WindowCombineState(windowSettings);
		
		return ret;
	}
}
