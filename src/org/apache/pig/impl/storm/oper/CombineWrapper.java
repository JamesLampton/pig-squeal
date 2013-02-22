package org.apache.pig.impl.storm.oper;

import java.util.List;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.pig.impl.io.NullableTuple;

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
			// FIXME: Create a MapWritable generator that always has the appropriate classes?
			ret.put(LAST, agg.combine(agg.zero(), val1.get(CUR)));
		}
		ret.put(CUR, agg.combine(getDefault(val1, CUR), getDefault(val2, CUR)));
			
		return ret;
	}

	@Override
	public MapWritable zero() {
		return new MapWritable();		
	}

	public static List<NullableTuple> getTuples(MapWritable m, Text which) {
		MapWritable state = (MapWritable) m.get(which);
		if (state == null) {
			return null;
		}
		return TriBasicPersist.getTuples(state);
	}	
}
