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
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.ReverseDependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.storm.SpoutWrapper;

/**
 * The purpose of this class is to find elements of a MapReduce plan that
 * contribute to replicated joins.  For our purposes, they will be rooted
 * in regular load functions and terminate in stores that are FRJoin files.
 * These chains need to be removed and executed before the Storm job.
 * 
 * @author jhl1
 *
 */
public class ReplJoinFixer extends MROpPlanVisitor {

	private MROperPlan plan;
	private MROperPlan replPlan = new MROperPlan();
	
	public MROperPlan getReplPlan() {
		return replPlan;
	}
	
	Map<String, MapReduceOper> fnToMOP = new HashMap<String, MapReduceOper>();
	Set<FileSpec> rFiles = new HashSet<FileSpec>();

//	private Set<FileSpec> replFiles = new HashSet<FileSpec>();
	
	public ReplJoinFixer(MROperPlan plan) {
		super(plan, new DependencyOrderWalker<MapReduceOper, MROperPlan>(plan));
		this.plan = plan;
	}
	
	class FRJoinFinder extends PhyPlanVisitor {
		private Set<FileSpec> replFiles;

		public FRJoinFinder(PhysicalPlan plan, Set<FileSpec> replFiles) {
			super(plan, new DependencyOrderWalker<PhysicalOperator, PhysicalPlan>(plan));
			this.replFiles = replFiles;
		}
		
	    @Override
	    public void visitFRJoin(POFRJoin join) throws VisitorException {
	    	// Extract the files.
	    	for (FileSpec f : join.getReplFiles()) {
	    		if (f != null) {
//	    			System.out.println("Join: " + join + " " + f);
	    			replFiles.add(f);
	    		}
	    	}
	    }
	}
	
	public void visitMROp(MapReduceOper mr) throws VisitorException {
		// Cycle through the leaves adding them to the fnToMOP.
		List<PhysicalOperator> leaves = new ArrayList<PhysicalOperator>(mr.mapPlan.size() + mr.reducePlan.size());
		leaves.addAll(mr.mapPlan.getLeaves());
		leaves.addAll(mr.reducePlan.getLeaves());
		for (PhysicalOperator po : leaves) {
			if (po instanceof POStore) {
				String fn = ((POStore)po).getSFile().getFileName();
//				System.out.println("OP: " + mr.getOperatorKey() + " file: " + fn);
				fnToMOP.put(fn, mr);
			}
		}

		// Collect the replicated files, we'll sweep back along fnToMOP later.
        new FRJoinFinder(mr.mapPlan, rFiles).visit();
        new FRJoinFinder(mr.reducePlan, rFiles).visit();        
	}
	
	public void convert() {
		// Start walking.
		try {
			visit();
			extractReplPlans();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void extractReplPlans() throws PlanException {
//		System.out.println("rFiles: " + rFiles);
		for (FileSpec f : rFiles) {
			// Determine the leaf of the plan that produces this file
			// and move the plan to replPlan.
			moveToReplPlan(fnToMOP.get(f.getFileName()));
		}
	}

	private void moveToReplPlan(MapReduceOper mr_cur) throws PlanException {
		// We're going to do this recursively.
		List<MapReduceOper> preds = plan.getPredecessors(mr_cur);
		
		// Remove the current operator.
		plan.remove(mr_cur);
		// Put it into the new plan.
		replPlan.add(mr_cur);
		
		if (preds == null) {
			return;
		}
		
		for (MapReduceOper pred : preds) {
			// Move all the predecessors.
			moveToReplPlan(pred);
			// And link in the new plan.
			replPlan.connect(pred, mr_cur);
		}
	}
}
