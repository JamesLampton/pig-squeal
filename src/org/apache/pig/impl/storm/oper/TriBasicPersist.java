package org.apache.pig.impl.storm.oper;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.util.ObjectSerializer;

import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

public class TriBasicPersist implements ReducerAggregator {
		
	@Override
	public Map<Object, Integer> init() {
		return new HashMap<Object, Integer>();
	}

	@Override
	public Map<Object, Integer> reduce(Object curr, TridentTuple tuple) {
		Map<Object, Integer> state = (Map<Object, Integer>) curr;
		if (state == null) {
			state = init();
		}
		
		Object values = tuple.get(1);
		
		// Serialize the tuple to a string to be used as the key into the state.
		try {			
//			String k = ObjectSerializer.serialize((Serializable) values);
			Integer p = state.get(values);
			state.put(values, (p == null) ? 1 : p + 1);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
		return state;
	}

	static public List<NullableTuple> getTuples(Object state_o) {
		List<NullableTuple> ret = new ArrayList<NullableTuple>();
		
		Map<Object, Integer> state = (Map<Object, Integer>) state_o;
		for (Entry<Object, Integer> ent : state.entrySet()) {
			int c = ent.getValue();
			NullableTuple v = (NullableTuple) ent.getKey();
			for (int i = 0; i < c; i++) {
				ret.add(v);
			}
		}
		
		return ret;
	}
}
