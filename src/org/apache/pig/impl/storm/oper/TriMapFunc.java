package org.apache.pig.impl.storm.oper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;

import backtype.storm.tuple.Values;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class TriMapFunc extends StormBaseFunction {

	private PhysicalPlan mapPlan;
	private List<PhysicalOperator> roots;
	private PhysicalOperator leaf;
	private boolean errorInMap;
	public byte mapKeyType;
	boolean computeKey;

	public TriMapFunc(PhysicalPlan physicalPlan, byte mapKeyType) {
		mapPlan = physicalPlan;
		roots = new ArrayList<PhysicalOperator>();
		List<PhysicalOperator> remove_roots = new ArrayList<PhysicalOperator>(mapPlan.getRoots());
		for (PhysicalOperator pl : remove_roots) {
			roots.addAll(mapPlan.getSuccessors(pl));
			// Remove the root so it can never be reached.
			mapPlan.remove(pl);
		}

		leaf = mapPlan.getLeaves().get(0);
		if (leaf.getClass().isAssignableFrom(POLocalRearrange.class)) {
			computeKey = true;
		}
		if (leaf.getClass().isAssignableFrom(POStore.class)) {
			// We need to actually peel the POStore off.
			leaf = mapPlan.getPredecessors(leaf).get(0);
		}
		this.mapKeyType = mapKeyType;
	}
	
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		// TODO: We'll pass the input through the first element in the tuple with multiple map inputs		
//		System.out.println("Map: " + tuple + " roots: " + roots + " leaf: " + leaf);
		
		// Bind the tuple.
		roots.get(0).attachInput((Tuple) tuple.get(1));
		
		// And pull the results from the leaf.
		try {
			runPipeLine(collector);
		} catch (ExecException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	private static final Tuple DUMMYTUPLE = null;
	
    public void collect(TridentCollector collector, Tuple tuple) throws ExecException {
//    	System.out.println("Map collect: " + tuple + " mapKeyType: " + mapKeyType);

    	if (computeKey) {
    		Byte index = (Byte) tuple.get(0);
    		PigNullableWritable key =
    				HDataType.getWritableComparableTypes(tuple.get(1), mapKeyType);
    		NullableTuple val = new NullableTuple((Tuple)tuple.get(2));

    		// Both the key and the value need the index.  The key needs it so
    		// that it can be sorted on the index in addition to the key
    		// value.  The value needs it so that POPackage can properly
    		// assign the tuple to its slot in the projection.
    		key.setIndex(index);
    		val.setIndex(index);

//    		System.out.println("Emit k: " + key + " -- v: " + val);
    		collector.emit(new Values(key, val));    		
    	} else {
    		collector.emit(new Values(null, tuple));
    	}
    }
	
	void runPipeLine(TridentCollector collector) throws ExecException {
		while(true){
            Result res = leaf.getNext(DUMMYTUPLE);
            if(res.returnStatus==POStatus.STATUS_OK){
            	
                collect(collector,(Tuple)res.result);
                continue;
            }
            
            if(res.returnStatus==POStatus.STATUS_EOP) {
                return;
            }
            
            if(res.returnStatus==POStatus.STATUS_NULL)
                continue;
            
            if(res.returnStatus==POStatus.STATUS_ERR){
                // remember that we had an issue so that in 
                // close() we can do the right thing
                errorInMap = true;
                // if there is an errmessage use it
                String errMsg;
                if(res.result != null) {
                    errMsg = "Received Error while " +
                    "processing the map plan: " + res.result;
                } else {
                    errMsg = "Received Error while " +
                    "processing the map plan.";
                }
                    
                int errCode = 2055;
                ExecException ee = new ExecException(errMsg, errCode, PigException.BUG);
                throw new RuntimeException(ee);
            }
        }
	}
}
