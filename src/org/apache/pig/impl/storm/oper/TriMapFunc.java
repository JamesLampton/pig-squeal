package org.apache.pig.impl.storm.oper;

import java.io.IOException;
import java.io.Serializable;
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
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.storm.plans.CombineInverter;

import backtype.storm.tuple.Values;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class TriMapFunc extends StormBaseFunction {

	private static final Tuple DUMMYTUPLE = null;
	private PlanExecutor mapPlanExecutor;
	private PlanExecutor negMapPlanExecutor = null;
	
	class PlanExecutor implements Serializable {
		private PhysicalOperator leaf;
		private boolean computeKey;
		private byte mapKeyType;
		private boolean errorInMap;
		private PhysicalOperator root;

		protected PlanExecutor(PhysicalPlan plan, byte mapKeyType, PhysicalOperator root) {
			this.root = root;
			
			leaf = plan.getLeaves().get(0);
			if (leaf.getClass().isAssignableFrom(POStore.class)) {
				// We need to actually peel the POStore off.
				leaf = plan.getPredecessors(leaf).get(0);
			} else {
				computeKey = true;
			}
			this.mapKeyType = mapKeyType;
		}
		
		public void execute(TridentTuple tuple, TridentCollector collector, Integer tive) {
			// Bind the tuple.
			root.attachInput((Tuple) tuple.get(1));
			
			// And pull the results from the leaf.
			try {
				runPipeLine(collector, tive);
			} catch (ExecException e) {
				throw new RuntimeException(e);
			}
		}
		
	    public void collect(TridentCollector collector, Tuple tuple, Integer tive) throws ExecException {
//	    	System.out.println("Map collect: " + tuple + " mapKeyType: " + mapKeyType);

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

//	    		System.out.println("Emit k: " + key + " -- v: " + val);
	    		collector.emit(new Values(key, val, tive));    		
	    	} else {
	    		collector.emit(new Values(null, new NullableTuple(tuple), tive));
	    	}
	    }
		
		void runPipeLine(TridentCollector collector, Integer tive) throws ExecException {
			while(true){
	            Result res = leaf.getNext(DUMMYTUPLE);
	            if(res.returnStatus==POStatus.STATUS_OK){
	            	
	                collect(collector, (Tuple)res.result, tive);
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

	public TriMapFunc(PhysicalPlan physicalPlan, byte mapKeyType, boolean isCombined, PhysicalOperator activeRoot) {
		// Pull out the active root and get the predecessor.
		List<PhysicalOperator> roots = physicalPlan.getSuccessors(activeRoot);
		if (roots.size() > 1) {
			throw new RuntimeException("Expected only a single successor from the active root.");
		}
//		System.out.println("TriMapFunc: " + activeRoot + " -> " + roots.get(0));
		activeRoot = roots.get(0);
		
		// Remove the root
		List<PhysicalOperator> remove_roots = new ArrayList<PhysicalOperator>(physicalPlan.getRoots());
		for (PhysicalOperator pl : remove_roots) {
			// Remove the root so it can never be reached.
			physicalPlan.remove(pl);
		}

		mapPlanExecutor = new PlanExecutor(physicalPlan, mapKeyType, activeRoot);
		
		// See if this plan requires a negative pipeline.
		if (isCombined) {
			CombineInverter ci = new CombineInverter(physicalPlan);
			try {
				PhysicalPlan ciClone = ci.getInverse();
				// track activeRoot through the clone.
				PhysicalOperator ciActiveRoot = ci.getLastOpMap().get(activeRoot).get(0);
				
				negMapPlanExecutor  = new PlanExecutor(ciClone, mapKeyType, ciActiveRoot);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}			
		}
	}
	
	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
//		System.out.println("Map: " + tuple + " activeRoot: " + mapPlanExecutor.root + " leaf: " + mapPlanExecutor.leaf);
		
		// Determine if the tuple is positive or negative
		Integer tive = tuple.getInteger(2);
		
		if (negMapPlanExecutor != null && tive < 0) {
			negMapPlanExecutor.execute(tuple, collector, tive);
		} else {
			mapPlanExecutor.execute(tuple, collector, tive);
		}
	}

}
