package org.apache.pig.impl.storm.plans;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PhyPlanSetter;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POJoinPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.storm.io.NOPLoad;
import org.apache.pig.impl.storm.io.SpoutWrapper;

public class MRtoSConverter extends MROpPlanVisitor {

	private MROperPlan plan;
	private SOperPlan splan;
	
	// TODO: Track these by the logical operators they represent.
	private Map<String, StormOper> rootMap = new HashMap<String, StormOper>();;
	private Map<String, List<StormOper>> missingRoots = new HashMap<String, List<StormOper>>();
	NodeIdGenerator nig = NodeIdGenerator.getGenerator();
	private String scope;
	private PigContext pc;
	
	public MRtoSConverter(MROperPlan plan, PigContext pc) {
		super(plan, new DependencyOrderWalker<MapReduceOper, MROperPlan>(plan));
		this.plan = plan;
		this.splan = new SOperPlan();
		this.pc = pc;
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
	
	static public String getAlias(PhysicalPlan p, boolean useRoots) {
//		System.out.println(useRoots + " Roots: " + p.getRoots() + " Leaves: " + p.getLeaves());
		if (useRoots) {
			PhysicalOperator root = p.getRoots().get(0);
			if (root instanceof POJoinPackage) {
				root = p.getSuccessors(root).get(0);
			}
			return (root == null) ? null : root.getAlias();
		}
		
		PhysicalOperator leaf = p.getLeaves().get(0);
		String alias = leaf.getAlias();
		if (leaf instanceof POStore) {
			leaf = p.getPredecessors(leaf).get(0);
		}
		return (leaf == null || leaf.getAlias() == null) ? alias : leaf.getAlias();
	}
	
	private class FRJoinFinder extends PhyPlanVisitor {

		private Set<FileSpec> replFiles;

		public FRJoinFinder(PhysicalPlan plan, Set<FileSpec> replFiles) {
			super(plan, new DependencyOrderWalker<PhysicalOperator, PhysicalPlan>(plan));
			this.replFiles = replFiles;
		}
		
	    @Override
	    public void visitFRJoin(POFRJoin join) throws VisitorException {
	    	// Extract the files.
	    	for (FileSpec f : join.getReplFiles()) {
	    		replFiles.add(f);
	    	}
	    }
	}
	
	public void visitMROp(MapReduceOper mr) throws VisitorException {
		splan.UDFs.addAll(mr.UDFs);
		
		new PhyPlanSetter(mr.mapPlan).visit();
        new PhyPlanSetter(mr.reducePlan).visit();
        
        new FRJoinFinder(mr.mapPlan, splan.replFiles).visit();
        new FRJoinFinder(mr.reducePlan, splan.replFiles).visit();        
        
		// Map SOP -- Attach to Spout or to Reduce SOP -- replace LOADs
		// Optional Spout Point (May hook onto the end of a Reduce SOP)
		StormOper mo = getSOp(StormOper.OpType.MAP, getAlias(mr.mapPlan, false));
		mo.mapKeyType = mr.mapKeyType;
		splan.add(mo);

		// Look for the input
		for (PhysicalOperator po : mr.mapPlan.getRoots()) {
			POLoad pl = (POLoad)po;
			
			if (pl instanceof NOPLoad) {
				// Skip the static stuff.
				continue;
			}
			
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
		// See if we're a window.
		String red_alias = (mr.combinePlan.size() > 0) ? getAlias(mr.combinePlan, false) : getAlias(mr.reducePlan, true);
		String window_opts = pc.getProperties().getProperty(red_alias + "_window_opts");
//		System.out.println("RED_ALIAS: " + red_alias + " window_opts: " + window_opts);
		if (mr.combinePlan.size() == 0 || window_opts != null) {
			// Basic reduce or windowed group operator.
			po = getSOp(StormOper.OpType.BASIC_PERSIST, red_alias);
			po.setWindowOptions(window_opts);
		} else {
			// Combine SOP
			po = getSOp(StormOper.OpType.COMBINE_PERSIST, getAlias(mr.combinePlan, false));
			po.setPlan(mr.combinePlan);
			po.mapKeyType = mr.mapKeyType;
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
		// Set window options to tell TriReduce which getTuples routine to call.
		rdo.setWindowOptions(window_opts);
		
//		System.err.println("Reduce leaves: " + mr.reducePlan.getLeaves());
		setupStore(mr.reducePlan.getLeaves(), rdo);
	}
	
	public void convert() {
		// Start walking.
		try {
			// Pull out any static subtrees from the execution plan.
			FixedLoadPathFixer flpf = new FixedLoadPathFixer(plan, pc);
			flpf.convert();
			if (flpf.getStaticPlan().size() > 0) {
				splan.setStaticPlan(flpf.getStaticPlan());
			}
			
			// Pull out the replicated join creation plan.
//			ReplJoinFixer rjf = new ReplJoinFixer(plan);
//			rjf.convert();
//			if (rjf.getReplPlan().size() > 0) {
//				splan.setReplPlan(rjf.getReplPlan());
//				splan.setReplFileMap(rjf.getReplFileMap());
//			}
			
			visit();
			splan.setRootMap(rootMap);
			
//			System.out.println("ReplFiles: " + splan.replFiles);
			if (missingRoots.size() > 0) {
				// We have some paths that aren't attached to the plan.
				System.out.println("Missing roots: " + missingRoots);
			}
		} catch (VisitorException e) {
			e.printStackTrace();
		}
//		System.out.println("here");
	}

	public SOperPlan getSPlan() {
		return splan;
	}
}
