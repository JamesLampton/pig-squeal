package org.apache.pig.impl.storm.io;

import java.util.HashMap;
import java.util.Iterator;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.storm.oper.CombineWrapper;
import org.apache.pig.impl.storm.oper.TriBasicPersist;
import org.apache.pig.impl.storm.oper.TriReduce;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import storm.trident.operation.CombinerAggregator;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.MapState;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTupleView;
import storm.trident.tuple.TridentTupleView.FreshOutputFactory;

public class TridentStatePack extends POPackage {

	private StateFactory stateFactory;
	boolean initialized = false;
	MapState s;
	CombinerAggregator agg;
	TridentTupleView.FreshOutputFactory tFactory;
	private final static Integer POS = 1;
	
	public TridentStatePack(OperatorKey k, StateFactory stateFactory) {
		super(k);
		this.stateFactory = stateFactory;
	}
	
	@Override
	public void attachInput(PigNullableWritable k, Iterator<NullableTuple> inp) {
		if (initialized == false) {
			initialized = true;
			s = (MapState) stateFactory.makeState(new HashMap(), 0, 1);
			agg = new CombineWrapper(new TriBasicPersist());
			tFactory = new TridentTupleView.FreshOutputFactory(new Fields("k", "v", "s"));
			
			System.out.println("TridentStatePack.attachInput initialized state: " + s + " agg: " + agg);
		}

		// Aggregate the values.
		Object state = null;
		while (inp.hasNext()) {
			NullableTuple t = inp.next();
			
			// Create a trident tuple.
			TridentTuple triTuple = tFactory.create(new Values(k, t, POS));
			
			// Initialize the current tuple t.
			Object t_init = agg.init(triTuple);
			
			// And combine
			if (state == null) {
				state = t_init;
			} else {
				state = agg.combine(state, t_init);
			}
		}
		
		// Stash it out to the state.
//		System.out.println("Writing: " + k);
//		s.beginCommit(new Long(0));
		s.multiPut(new Values(new Values(k)), new Values(state));
//		s.commit(new Long(0));
		
//		System.out.println("TridentStatePack.attachInput called -- State: " + s);
	}
	
	@Override
	public Result getNext(Tuple t) throws ExecException {
		// All the trickery is in attach input.
		Result res = new Result();
		res.returnStatus = POStatus.STATUS_EOP;
		return res;
	}
}
