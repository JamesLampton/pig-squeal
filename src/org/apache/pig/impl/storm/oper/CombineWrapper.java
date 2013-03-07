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
import storm.trident.tuple.TridentTuple;

public class CombineWrapper implements CombinerAggregator<MapWritable> {
	private CombinerAggregator<Writable> agg;
	static public final Text CUR = new Text("cur");
	static public final Text LAST = new Text("last");
	
	public CombineWrapper(CombinerAggregator agg) {
		this.agg = (CombinerAggregator<Writable>) agg;
	}

	@Override
	public MapWritable init(TridentTuple tuple) {
		MapWritable ret = zero();
		ret.put(CUR, agg.init(tuple));
		return ret;
	}

	Writable getDefault(MapWritable v, Text k) {
		if (v.containsKey(k)) {
			return v.get(k);
		}
		return agg.zero();
	}
	
	@Override
	public MapWritable combine(MapWritable val1, MapWritable val2) {
//		System.out.println("combine: " + val1 + " " + val2);
		MapWritable ret = zero();

		// Assuming that val1 came from the cache/state.
		if (val1.get(CUR) != null) {
			ret.put(LAST, val1.get(CUR));
		}
		ret.put(CUR, agg.combine(getDefault(val1, CUR), getDefault(val2, CUR)));
			
		return ret;
	}

	@Override
	public MapWritable zero() {
		return new MapWritable();		
	}

	public static List<NullableTuple> getTuples(MapWritable m, Text which) {

		Writable state = m.get(which);
		if (state == null) {
			return null;
		}
		
		List<NullableTuple> ret;

		if (state instanceof MapWritable) {
			ret = TriBasicPersist.getTuples((MapWritable) state);
		} else {
			ret = TriCombinePersist.getTuples((CombineTupleWritable) state);
		}
		
		// Sort the tuples as the shuffle would.
		Collections.sort(ret);
		
		return ret;
	}	
}
