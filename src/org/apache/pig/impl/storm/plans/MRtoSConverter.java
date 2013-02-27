package org.apache.pig.impl.storm.plans;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PhyPlanSetter;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.storm.SpoutWrapper;

public class MRtoSConverter extends MROpPlanVisitor {

	private MROperPlan plan;
	private SOperPlan splan;
	
	// TODO: Track these by the logical operators they represent.
	private Map<String, StormOper> rootMap = new HashMap<String, StormOper>();;
	private Map<String, List<StormOper>> missingRoots = new HashMap<String, List<StormOper>>();
	NodeIdGenerator nig = NodeIdGenerator.getGenerator();
	private String scope;;
	
	public MRtoSConverter(MROperPlan plan) {
		super(plan, new DepthFirstWalker<MapReduceOper, MROperPlan>(plan));
		this.plan = plan;
		this.splan = new SOperPlan();
		scope = plan.getRoots().get(0).getOperatorKey().getScope();
	}
	
	private StormOper getSOp(StormOper.OpType t, String alias){
        return new StormOper(new OperatorKey(scope,nig.getNextNodeId(scope)), t, alias);
    }
	
	void setupStore(List<PhysicalOperator> list, StormOper sop) {
		for (PhysicalOperator op : list) {
			if (op.getClass().isAssignableFrom(POStore.class)) {
				POStore ps = (POStore) op;
				String fn = ps.getSFile().getFileName();
				if (missingRoots.containsKey(fn)) {
					for (StormOper waiting : missingRoots.remove(fn)) {
						try {
							splan.connect(sop, waiting);
						} catch (PlanException e) {
							e.printStackTrace();
						}
					}
				}
				rootMap.put(fn, sop);
			}
		}
	}
	
	String getAlias(PhysicalPlan p, boolean useRoots) {
//		System.out.println(useRoots + " Roots: " + p.getRoots() + " Leaves: " + p.getLeaves());
		if (useRoots) {
			PhysicalOperator root = p.getRoots().get(0);
			return root.getAlias();
		}
		
		PhysicalOperator leaf = p.getLeaves().get(0);
		if (leaf instanceof POStore) {
			leaf = p.getPredecessors(leaf).get(0);
		}
		return (leaf == null) ? null : leaf.getAlias();
	}
	
	public void visitMROp(MapReduceOper mr) throws VisitorException {
		splan.UDFs.addAll(mr.UDFs);
		
		new PhyPlanSetter(mr.mapPlan).visit();
        new PhyPlanSetter(mr.reducePlan).visit();
		
		// Map SOP -- Attach to Spout or to Reduce SOP -- replace LOADs
		// Optional Spout Point (May hook onto the end of a Reduce SOP)
		StormOper mo = getSOp(StormOper.OpType.MAP, getAlias(mr.mapPlan, false));
		mo.mapKeyType = mr.mapKeyType;
		splan.add(mo);

		// Look for the input
		for (PhysicalOperator po : mr.mapPlan.getRoots()) {
			POLoad pl = (POLoad)po;
//			System.err.println(po);
			String fn = pl.getLFile().getFileName();
			
			// If it's a Spout, add a new StormOper for it.
			if (!rootMap.containsKey(fn) && pl.getLoadFunc() instanceof SpoutWrapper) {
				// Create a new StormOper for this spout.
				StormOper spout = getSOp(StormOper.OpType.SPOUT, po.getAlias());
				SpoutWrapper sw = ((SpoutWrapper)pl.getLoadFunc());
				spout.setSpout(sw.getSpout());
				splan.add(spout);
				splan.addPLSpoutLink(spout, pl);
				rootMap.put(fn, spout);

				// Add the spout's UDF so it gets picked up by the Jar.
				splan.UDFs.add(sw.getSpoutClass());
			}
			
			if (rootMap.containsKey(fn)) {
				// Add the MapPlan as a dependency on the Stream.
				try {
					splan.connect(rootMap.get(fn), mo);
				} catch (PlanException e) {
					e.printStackTrace();
				}
			} else {
				List<StormOper> wait = missingRoots.get(po);
				if (wait == null) {
					wait = new ArrayList<StormOper>();
					missingRoots.put(fn, wait);
				}
				// Add this map to the waiting list for the Stream
				wait.add(mo);
			}
		}
		
		// Map: We have out input squared away, lets create the foreach function.
		// TODO -- Replace loads
		mo.setPlan(mr.mapPlan);
		if (mr.combinePlan.size() > 0) {
			mo.isCombined = true;
		}
		
		// If this is a Map-only task, we need to find the store and create the link for the output.
		if (mr.reducePlan.size() == 0) {
			setupStore(mr.mapPlan.getLeaves(), mo); // Can a map have leaves if in a MR job?
			return;
		}
		
		// Persist SOP
		StormOper po;
		if (mr.combinePlan.size() > 0) {
			po = getSOp(StormOper.OpType.COMBINE_PERSIST, getAlias(mr.combinePlan, false));
			// Combine SOP
			po.setPlan(mr.combinePlan);
			po.mapKeyType = mr.mapKeyType;
		} else {
			// Basic store SOP
			po = getSOp(StormOper.OpType.BASIC_PERSIST, getAlias(mr.reducePlan, true));
		}
		splan.add(po);
		try {
			splan.connect(mo, po);
		} catch (PlanException e) {
			e.printStackTrace();
		}
		
		// Reduce SOP
		StormOper rdo = getSOp(StormOper.OpType.REDUCE_DELTA, getAlias(mr.reducePlan, false));
		splan.add(rdo);
		try {
			splan.connect(po, rdo);
		} catch (PlanException e) {
			e.printStackTrace();
		}
		// TODO -- Remove stores.
		rdo.setPlan(mr.reducePlan);
		
//		System.err.println("Reduce leaves: " + mr.reducePlan.getLeaves());
		setupStore(mr.reducePlan.getLeaves(), rdo);
	}
	
	public void convert() {
		// Start walking.
		try {
			visit();
		} catch (VisitorException e) {
			e.printStackTrace();
		}
//		System.out.println("here");
	}
	
	public SOperPlan getSPlan() {
		return splan;
	}
}
