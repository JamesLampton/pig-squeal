package org.apache.pig.impl.storm.plans;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
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
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.PlanWalker;
import org.apache.pig.impl.plan.ReverseDependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.storm.io.SpoutWrapper;

/**
 * The purpose of this class is to find fixed elements within the MapReduce
 * plan and to execute Hadoop jobs to place the appropriate data into the
 * Trident State.
 * 
 * We're looking for non-streaming wrapped loads that ultimately mix with
 * streamed results.
 * 
 * @author jhl1
 *
 */
public class FixedLoadPathFixer extends MROpPlanVisitor {

	private MROperPlan plan;
	private MROperPlan staticPlan = new MROperPlan();

	Set<String> staticFiles = new HashSet<String>();
	Set<MapReduceOper> staticMOPs = new HashSet<MapReduceOper>();
	Map<String, MapReduceOper> fnToMOP = new HashMap<String, MapReduceOper>();
	Map<String, MapReduceOper> staticFnToMOP = new HashMap<String, MapReduceOper>();
	List<MapReduceOper> mixMOP = new ArrayList<MapReduceOper>();
	
	//	Map<FileSpec, FileSpec> rFileMap = new HashMap<FileSpec, FileSpec>();

	public FixedLoadPathFixer(MROperPlan plan) {
		super(plan, new DependencyOrderWalker<MapReduceOper, MROperPlan>(plan));
		this.plan = plan;
	}

	class LoadFinder extends PhyPlanVisitor {
		List<POLoad> ll;		
		public LoadFinder(PhysicalPlan plan, List<POLoad> load_list) {
			super(plan, new DependencyOrderWalker<PhysicalOperator, PhysicalPlan>(plan));
			this.ll = load_list;
		}

		@Override 
		public void visitLoad(POLoad load) {
			ll.add(load);
		}
	}

	public void visitMROp(MapReduceOper mr) throws VisitorException {
		// Look at the load functions and determine if they come from static sources.
		List<POLoad> load_list = new ArrayList<POLoad>();
		new LoadFinder(mr.mapPlan, load_list).visit();
		int streamedInputCount = 0;
		int staticInputCount = 0;

		// We will build a static plan and prune the dynamic plan.
		for (POLoad load : load_list) {
			String fn = load.getLFile().getFileName();
			if (load.getLoadFunc() instanceof SpoutWrapper) {
				// This data comes from a path with a streaming component.
				streamedInputCount += 1;
				System.out.println("STREAMING DIRECT LOAD: " + fn);
			} else if (fnToMOP.containsKey(fn)) {
				// This file is an intermediate result and may come from streaming input.
				if (staticFiles.contains(fn)) {
					System.out.println("INTERMEDIATE STATIC LOAD: " + load);
					staticInputCount += 1;
				} else {
					System.out.println("INTERMEDIATE STREAM LOAD: " + load);					
					streamedInputCount += 1;
				}
			} else {
				// Completely static, replace with a NOP Loader for the streaming plan.
				System.out.println("STATIC LOAD: " + load);
				staticInputCount += 1;				
			}
		}
		
		List<PhysicalOperator> leaves = new ArrayList<PhysicalOperator>(mr.mapPlan.size() + mr.reducePlan.size());
		leaves.addAll(mr.mapPlan.getLeaves());
		leaves.addAll(mr.reducePlan.getLeaves());
		for (PhysicalOperator po : leaves) {
			if (po instanceof POStore) {
				String fn = ((POStore)po).getSFile().getFileName();
				fnToMOP.put(fn, mr);

				// This MROP contains no streamed input and can be executed
				// plainly as a Hadoop job.
				if (streamedInputCount == 0) {
					staticFiles.add(fn);
					staticFnToMOP.put(fn, mr);
					staticMOPs.add(mr);
				} else if (streamedInputCount > 0 && staticInputCount > 0) {
					// This operator is a mixing operator where static and dynamic data meet.
					mixMOP.add(mr);
				}
			}
		}
	}

	public void convert() {
		// Start walking.
		try {
			visit();
			System.out.println("STATIC FILES: " + staticFiles);
			for (MapReduceOper mr : mixMOP) {
				System.out.println("MIX OP: " + mr.getOperatorKey());
			}
			for (MapReduceOper mr : staticMOPs) {
				System.out.println("Static OP: " + mr.getOperatorKey());
			}
			extractStaticPlans();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	private void extractStaticPlans() throws PlanException, VisitorException, CloneNotSupportedException {
		for (MapReduceOper mr : mixMOP) {
			System.out.println("MIX OP -- PROC: " + mr.getOperatorKey());
			
			List<MapReduceOper> preds = new ArrayList<MapReduceOper>();
			if (plan.getPredecessors(mr) != null) {
				preds.addAll(plan.getPredecessors(mr));
			}
			List<MapReduceOper> static_preds = new ArrayList<MapReduceOper>();
			// Look at the predecessors for static trees.
			for (MapReduceOper pre : preds) {
				if (staticMOPs.contains(pre)) {
					// Add to a list of static elements.
					static_preds.add(pre);
					// Prune this tree from the plan.
					moveToStaticPlan(pre);					
				}
			}
			
			// Pull the loads for this operator to replace them.
			List<POLoad> load_list = new ArrayList<POLoad>();
			new LoadFinder(mr.mapPlan, load_list).visit();
			
			// Add a new step to the static plan to load
			// the data into a trident state.
			addLoadStateOper(mr, static_preds, load_list);
			
			// Replace all the static loads with NOP loaders.
			// TODO
		}
	}
	
	private void moveToStaticPlan(MapReduceOper mr_cur) throws PlanException {
		System.out.println("moveToStaticPlan: " + mr_cur.getOperatorKey());
		
		// We're going to do this recursively.
		List<MapReduceOper> preds = plan.getPredecessors(mr_cur);

		// Remove the current operator.
		plan.remove(mr_cur);
		// Put it into the new plan.
		staticPlan.add(mr_cur);

		if (preds == null) {
			return;
		}

		for (MapReduceOper pred : preds) {
			// Move all the predecessors.
			moveToStaticPlan(pred);
			// And link in the new plan.
			staticPlan.connect(pred, mr_cur);
		}
	}
	
	private void addLoadStateOper(MapReduceOper mr, List<MapReduceOper> static_preds, List<POLoad> load_list) throws CloneNotSupportedException, PlanException {
		// We're going to create a new operator to stash the results from the static tree.
		String scope = mr.getOperatorKey().getScope();
		MapReduceOper state_mr = new MapReduceOper(new OperatorKey(scope, NodeIdGenerator.getGenerator().getNextNodeId(scope)));

		// Clone the Map plan from mr.
		state_mr.mapPlan = mr.mapPlan.clone();
		state_mr.mapKeyType = mr.mapKeyType;
		
		// Replace the non-static loads with NOP loaders.
		// TODO
		
		// Create a new reduce plan that stores the data into the state.
		// TODO
		state_mr.reducePlan = null;
		
		// Add the dependencies to using the static preds.
		staticPlan.add(state_mr);
		for (MapReduceOper pre : static_preds) {
			staticPlan.connect(pre, state_mr);
		}
	}

	public MROperPlan getStaticPlan() {
		return staticPlan;
	}
}
