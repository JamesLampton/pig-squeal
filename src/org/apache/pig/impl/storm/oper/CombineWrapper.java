package org.apache.pig.impl.storm.oper;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class CombineWrapper implements CombinerAggregator<MapWritable> {
	private CombinerAggregator<Writable> agg;
	static Text cur = new Text("cur");
	static Text last = new Text("last");
	
	public CombineWrapper(CombinerAggregator<Writable> agg) {
		this.agg = agg;
	}

	@Override
	public MapWritable init(TridentTuple tuple) {
		MapWritable ret = zero();
		ret.put(cur, agg.init(tuple));
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
		MapWritable ret = zero();

		// Assuming that val1 came from the cache/state.
		ret.put(last, val1.get(cur));
		ret.put(cur, agg.combine(getDefault(val1, cur), getDefault(val2, cur)));
			
		return ret;
	}

	@Override
	public MapWritable zero() {
		return new MapWritable();		
	}	
}
