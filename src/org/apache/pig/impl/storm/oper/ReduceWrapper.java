package org.apache.pig.impl.storm.oper;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.storm.state.CombineTupleWritable;

import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

public class ReduceWrapper implements ReducerAggregator<MapWritable> {
	private ReducerAggregator<Writable> agg;
	static public final Text CUR = new Text("cur");
	static public final Text LAST = new Text("last");
	
	public ReduceWrapper(ReducerAggregator agg) {
		this.agg = (ReducerAggregator<Writable>) agg;
	}

	Writable getDefault(MapWritable v, Text k) {
		if (v.containsKey(k)) {
			return v.get(k);
		}
		return agg.init();
	}

	public static List<NullableTuple> getTuples(MapWritable m, Text which) {
		Writable state = m.get(which);
		if (state == null) {
			return null;
		}
		
		List<NullableTuple> ret;

		ret = TriWindowPersist.getTuples((MapWritable)state);
				
		// Sort the tuples as the shuffle would.
		Collections.sort(ret);
		
		return ret;
	}

	@Override
	public MapWritable init() {
		return new MapWritable();
	}

	@Override
	public MapWritable reduce(MapWritable curr, TridentTuple tuple) {
		if (curr == null) {
			curr = init();
		}
		
		// Setup LAST if we've been run before.
		if (curr.get(CUR) != null) {
			curr.put(LAST, curr.get(CUR));
		}
		
		curr.put(CUR, agg.reduce(getDefault(curr, CUR), tuple));
		
		return curr;

	}	
}
