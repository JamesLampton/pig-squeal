package org.apache.pig.impl.storm.oper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POJoinPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;

import backtype.storm.tuple.Values;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class TriReduce extends StormBaseFunction {

	private PhysicalPlan reducePlan;
	private PhysicalOperator[] roots;
	private PhysicalOperator leaf;
	private POPackage pack;
	private boolean errorInReduce;
	private final static Tuple DUMMYTUPLE = null;
	private final static PhysicalOperator[] DUMMYROOTARR = {};
	
	public TriReduce(PhysicalPlan plan) {
		// We need to trim things from the plan re:GenericMapReduce.java
		reducePlan = plan;
		pack = (POPackage) plan.getRoots().get(0);
		plan.remove(pack);
//		keyType = mapKeyType;
		roots = plan.getRoots().toArray(DUMMYROOTARR);
		
		leaf = plan.getLeaves().get(0);
//		System.out.println("TriReduce roots: " + roots + " leaf: " + leaf + " isEmpty: " + reducePlan.isEmpty());
		if (leaf.getClass().isAssignableFrom(POStore.class)) {
			// We need to actually peel the POStore off.
			if (reducePlan.getPredecessors(leaf) != null) {
				leaf = reducePlan.getPredecessors(leaf).get(0);
			} else {
//				System.out.println("Leaf is null!");
				leaf = null;
			}
		}
	}
	
	@Override
	public void execute(TridentTuple tri_tuple, TridentCollector collector) {
//		System.out.println("TriReduce input: " + tri_tuple);
		
		PigNullableWritable key = (PigNullableWritable) tri_tuple.get(0);
		
		Object vl = tri_tuple.get(1);
		List<NullableTuple> tuples = new ArrayList<NullableTuple>();
		if (vl instanceof NullableTuple) {
			// Combine input
			tuples.add((NullableTuple)(vl));
		} else if (vl instanceof MapWritable) {
			// BasicPersist input
			tuples = TriBasicPersist.getTuples(vl);
		}
		
		try {
			if (pack instanceof POJoinPackage)
			{
				pack.attachInput(key, tuples.iterator());
				while (true)
				{
					if (processOnePackageOutput(collector))
						break;
				}
			}
			else {
				// join is not optimized, so package will
				// give only one tuple out for the key
				pack.attachInput(key, tuples.iterator());
				processOnePackageOutput(collector);
			} 
		} catch (ExecException e) {
			throw new RuntimeException(e);
		}
	}

	
	public boolean processOnePackageOutput(TridentCollector collector) throws ExecException  {
        Result res = pack.getNext(DUMMYTUPLE);
        if(res.returnStatus==POStatus.STATUS_OK){
            Tuple packRes = (Tuple)res.result;
            
            if(leaf == null || reducePlan.isEmpty()){
                collector.emit(new Values(null, packRes));
                return false;
            }
            for (int i = 0; i < roots.length; i++) {
                roots[i].attachInput(packRes);
            }
            runPipeline(leaf, collector);
        }
        
        if(res.returnStatus==POStatus.STATUS_NULL) {
            return false;
        }
        
        if(res.returnStatus==POStatus.STATUS_ERR){
            int errCode = 2093;
            String msg = "Encountered error in package operator while processing group.";
            throw new ExecException(msg, errCode, PigException.BUG);
        }
        
        if(res.returnStatus==POStatus.STATUS_EOP) {
            return true;
        }
            
        return false;
    }
    
    /**
     * @param leaf
     * @param collector 
     * @throws ExecException 
     */
    protected void runPipeline(PhysicalOperator leaf, TridentCollector collector) throws ExecException {
        
        while(true)
        {
            Result redRes = leaf.getNext(DUMMYTUPLE);
            if(redRes.returnStatus==POStatus.STATUS_OK){
                collector.emit(new Values(null, (Tuple)redRes.result));
                continue;
            }
            
            if(redRes.returnStatus==POStatus.STATUS_EOP) {
                return;
            }
            
            if(redRes.returnStatus==POStatus.STATUS_NULL) {
                continue;
            }
            
            if(redRes.returnStatus==POStatus.STATUS_ERR){
                // remember that we had an issue so that in 
                // close() we can do the right thing
                errorInReduce = true;
                // if there is an errmessage use it
                String msg;
                if(redRes.result != null) {
                    msg = "Received Error while " +
                    "processing the reduce plan: " + redRes.result;
                } else {
                    msg = "Received Error while " +
                    "processing the reduce plan.";
                }
                int errCode = 2090;
                throw new ExecException(msg, errCode, PigException.BUG);
            }
        }
    }
}
