package org.apache.pig.impl.storm;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.storm.oper.CombineWrapper;
import org.apache.pig.impl.storm.oper.ReduceWrapper;
import org.apache.pig.impl.storm.oper.TriBasicPersist;
import org.apache.pig.impl.storm.oper.TriCombinePersist;
import org.apache.pig.impl.storm.oper.TriMakePigTuples;
import org.apache.pig.impl.storm.oper.TriMapFunc;
import org.apache.pig.impl.storm.oper.TriReduce;
import org.apache.pig.impl.storm.oper.TriWindowCombinePersist;
import org.apache.pig.impl.storm.oper.TriWindowPersist;
import org.apache.pig.impl.storm.plans.SOpPlanVisitor;
import org.apache.pig.impl.storm.plans.SOperPlan;
import org.apache.pig.impl.storm.plans.StormOper;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.impl.util.ObjectSerializer;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.fluent.GroupedStream;
import storm.trident.operation.builtin.Debug;

public class Main {
	
	PigContext pc;
	SOperPlan splan;
	private TridentTopology t;
	private static final Log log = LogFactory.getLog(Main.class);
	
	public Main() {
		this(null, null);
	}
	
	public Main(PigContext pc, SOperPlan splan) {
		this.pc = pc;
		this.splan = splan;
		if (splan != null) {
			t = setupTopology(pc);
		}
	}
	
	public void initFromPigContext(PigContext pc) throws IOException {
		this.pc = pc;
		// Decode the plan from the context.
		splan = (SOperPlan) ObjectSerializer.deserialize(pc.getProperties().getProperty(StormLauncher.PLANKEY));
		t = setupTopology(pc);
	}
	
	class DepWalker extends SOpPlanVisitor {

		private TridentTopology topology;
		private Map<StormOper, Stream> sop_streams = new HashMap<StormOper, Stream>();
		private PigContext pc;
		
		protected DepWalker(TridentTopology topology, SOperPlan plan, PigContext pc) {
			super(plan, new DependencyOrderWalker<StormOper, SOperPlan>(plan));
			this.topology = topology;
			this.pc = pc;
		}
		
		Stream processMapSOP(StormOper sop) throws CloneNotSupportedException {
			Fields output_fields = sop.getOutputFields();			
			List<Stream> outputs = new ArrayList<Stream>();
			Stream output;
			
			// Cycle through the inputs and create a clone map for each.
			// This handles the cases for multiple inputs without breaking the plan apart.
			for (PhysicalOperator po : sop.getPlan().getRoots()) {
				StormOper input_sop = splan.getInputSOP((POLoad) po);
//				splan.getPLSpoutLink((POLoad) po);
				Stream input = sop_streams.get(input_sop);
				
				if (input == null) {
					// Probably a static load.
					continue;
				}
				
				MultiMap<PhysicalOperator, PhysicalOperator> opmap = new MultiMap<PhysicalOperator, PhysicalOperator>();
				sop.getPlan().setOpMap(opmap);
				PhysicalPlan clonePlan = sop.getPlan().clone();
				if (opmap.get(po).size() > 1) {
					throw new RuntimeException("Didn't expect activeRoot to have multiple values in cloned plan!");
				}
				PhysicalOperator cloneActiveRoot = opmap.get(po).get(0);
				
//				System.out.println("processMapSOP -- input: " + input + " " + input_sop + " " + po);
				output = input.each(
							input.getOutputFields(),
							new TriMapFunc(pc, clonePlan, sop.mapKeyType, sop.getIsCombined(), cloneActiveRoot),
							output_fields
						).project(output_fields);
				outputs.add(output);
			}
			
			if (outputs.size() == 1) {
				output = outputs.get(0);
			} else {
				output = topology.merge(outputs);
			}
			
			// Optional debug.
//			output.each(output.getOutputFields(), new Debug());
			
			return output;
		}
		
		List<Stream> getInputs(StormOper sop) {
			// Change to list?
			List<Stream> inputs = new ArrayList<Stream>();
			
			// Link to the previous streams.
			for (StormOper pre_sop : splan.getPredecessors(sop)) {
				inputs.add(sop_streams.get(pre_sop));
			}

			return inputs;
		}

		public void visitSOp(StormOper sop) throws VisitorException {
			Stream output = null;
			Fields output_fields = sop.getOutputFields();
			
			if (sop.getType() == StormOper.OpType.SPOUT) {
				output = topology.newStream(sop.getOperatorKey().toString(), sop.getLoadFunc());
				
				// Allow more than one to run.
				if (sop.getParallelismHint() != 0) {
					output.parallelismHint(sop.getParallelismHint());
				}
				
				// Add the conversion routine to the end to switch from Storm to Pig tuples.
				output = output.each(
							output.getOutputFields(), 
							new TriMakePigTuples(), 
							output_fields)
						.project(output_fields);
				
				sop_streams.put(sop, output);

				return;
			}

			// Default value for non-maps.
			Stream input = getInputs(sop).get(0);
			
			// Create the current operator on the topology
			if (sop.getType() == StormOper.OpType.MAP) {
				try {
					output = processMapSOP(sop);
//					output.each(output.getOutputFields(), new Debug());
				} catch (CloneNotSupportedException e) {
					throw new RuntimeException(e);
				}
			} else if (sop.getType() == StormOper.OpType.BASIC_PERSIST || sop.getType() == StormOper.OpType.COMBINE_PERSIST) {
				// We need to encode the key into a value (sans index) to group properly.
				Fields orig_input_fields = input.getOutputFields();
				Fields group_key = new Fields(input.getOutputFields().get(0) + "_raw");
				input = input.each(
							new Fields(input.getOutputFields().get(0)),
							new TriMapFunc.MakeKeyRawValue(),
							group_key
						);
				
				// Setup the aggregator.
				CombineWrapper agg = null;
				if (sop.getType() == StormOper.OpType.BASIC_PERSIST) {
					if (sop.getWindowOptions() == null) {
						agg = new CombineWrapper(new TriBasicPersist());
					} else {
						// We'll be windowing things.
						agg = new CombineWrapper(new TriWindowCombinePersist(sop.getWindowOptions()));
					}
				} else {					
					// We need to trim things from the plan re:PigCombiner.java
					POPackage pack = (POPackage) sop.getPlan().getRoots().get(0);
					sop.getPlan().remove(pack);

					agg = new CombineWrapper(new TriCombinePersist(pack, sop.getPlan(), sop.mapKeyType)); 
				}

				// Group and aggregate
				TridentState gr_persist = input.groupBy(group_key)
						.persistentAggregate(
								sop.getStateFactory(pc),
								orig_input_fields,
								agg, 
								output_fields
								);
				if (sop.getParallelismHint() > 0) {
					gr_persist.parallelismHint(sop.getParallelismHint());
				}
				output = gr_persist.newValuesStream();
			
				// Re-alias the raw as the key.
				output = output.each(
							group_key,
							new TriMapFunc.Copy(),
							new Fields(orig_input_fields.get(0))
						);

				// Strip down to the appropriate values
				output = output.project(new Fields(orig_input_fields.get(0), output_fields.get(0)));
//				output.each(output.getOutputFields(), new Debug());
			} else if (sop.getType() == StormOper.OpType.REDUCE_DELTA) {
				// Need to reduce
				output = input.each(
							input.getOutputFields(), 
							new TriReduce(pc, sop.getPlan(), (sop.getWindowOptions() == null ? false : true)), 
							output_fields
						).project(output_fields);
				output.each(output.getOutputFields(), new Debug());
			}
			
			sop_streams.put(sop, output);
			
			System.out.println("input fields: " + input.getOutputFields());
			System.out.println("output fields: " + output.getOutputFields());
		}
	};
	
	public TridentTopology setupTopology(PigContext pc) {
		TridentTopology topology = new TridentTopology();
		
		// Walk the plan and create the topology.
		DepWalker w = new DepWalker(topology, splan, pc);
		try {
			w.visit();
		} catch (VisitorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
//		for (StormOper r : splan.getRoots()) {	
//			// Grab the load statement.
//			System.err.println(r);
//		}
		
		return topology;
	}
	
	void runTestCluster(String topology_name, long wait_time, boolean debug) {
		// Run test.
		Map conf = new HashMap();
		conf.put(Config.TOPOLOGY_WORKERS, 1);
		conf.put(Config.TOPOLOGY_DEBUG, debug);
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(topology_name, conf, t.build());
		
		if (wait_time > 0) {
			Utils.sleep(wait_time);
			cluster.killTopology(topology_name);
			cluster.shutdown();
		}
	}
	
	public void launch() {
		// FIXME: Call out to storm jar.
		
		if (pc.getProperties().getProperty("pig.streaming.run.test.cluster", "false").equalsIgnoreCase("true")) {
			log.info("Running test cluster...");
			
			boolean debug = pc.getProperties().getProperty("pig.streaming.debug", "false").equalsIgnoreCase("true");
			int wait_time = Integer.parseInt(pc.getProperties().getProperty("pig.streaming.run.test.cluster.wait_time", "10000"));

			String topology_name = pc.getProperties().getProperty("pig.streaming.topology.name", "PigStorm-" + pc.getLastAlias());
			
			runTestCluster(topology_name, wait_time, debug);
			
			log.info("Back from test cluster.");
		}
	}
	
	Object getStuff(String name) {
		System.out.println(getClass().getClassLoader().getResource("pigContext"));
		ObjectInputStream fh;
		Object o = null;
		try {
			fh = new ObjectInputStream(getClass().getClassLoader().getResourceAsStream(name));
			o = fh.readObject();
			fh.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return o;
	}
	
	public void runMain(String[] args) throws IOException {
		/* Create the Pig context */
		pc = (PigContext) getStuff("pigContext");
		initFromPigContext(pc);
		launch();
	}
	
	public static void main(String[] args) throws Exception {
		Main m = new Main();
		m.runMain(args);
		
//		if (args.length > 0) {
//			Map conf = new HashMap();
//			int workers = 2;
//			conf.put(Config.TOPOLOGY_WORKERS, workers);
//			StormSubmitter submitter = new StormSubmitter();
//			
//			submitter.submitTopology(args[0], conf, setupTopology("NMTwitterInput", workers).build());
//		} else {
//			// Run test.
//			Map conf = new HashMap();
//			conf.put(Config.TOPOLOGY_WORKERS, 1);
////		    conf.put(Config.STORM_CLUSTER_MODE, "distributed"); 
//			//conf.put(Config.TOPOLOGY_DEBUG, true);
//
//			LocalCluster cluster = new LocalCluster();
//			cluster.submitTopology("nationmind", conf, setupTopology(null, 1).build());
//			/*
//			Utils.sleep(60000);
//			cluster.shutdown();
//			*/
//		}
	}
}
