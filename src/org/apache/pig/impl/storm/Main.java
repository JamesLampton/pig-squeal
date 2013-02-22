package org.apache.pig.impl.storm;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.DependencyOrderWalker;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.storm.oper.CombineWrapper;
import org.apache.pig.impl.storm.oper.TriBasicPersist;
import org.apache.pig.impl.storm.oper.TriCombinePersist;
import org.apache.pig.impl.storm.oper.TriMakePigTuples;
import org.apache.pig.impl.storm.oper.TriMapFunc;
import org.apache.pig.impl.storm.oper.TriReduce;
import org.apache.pig.impl.storm.plans.SOpPlanVisitor;
import org.apache.pig.impl.storm.plans.SOperPlan;
import org.apache.pig.impl.storm.plans.StormOper;
import org.apache.pig.impl.util.ObjectSerializer;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import storm.trident.Stream;
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

		public void visitSOp(StormOper sop) throws VisitorException {
			Stream input = null;
			Stream output = null;
			Fields output_fields = sop.getOutputFields();
			// Change to list?
			List<Stream> inputs = new ArrayList<Stream>();

			if (sop.getType() == StormOper.OpType.SPOUT) {
				output = topology.newStream(sop.getOperatorKey().toString(), sop.getLoadFunc());
				
				// Add the conversion routine to the end to switch from Storm to Pig tuples.
				output = output.each(
							output.getOutputFields(), 
							new TriMakePigTuples(), 
							output_fields)
						.project(output_fields);
				
				sop_streams.put(sop, output);

				return;
			}
			
			// Link to the previous streams.
			for (StormOper pre_sop : splan.getPredecessors(sop)) {
				inputs.add(sop_streams.get(pre_sop));
			}
			
			if (inputs.size() == 1) {
				input = inputs.get(0);
			} else {
				input = topology.merge(inputs);
			}

			// Create the current operator on the topology
			if (sop.getType() == StormOper.OpType.MAP) {
				// FIXME: Make all inputs SOPs so we can track the input fields for MAP.
				output = input.each(
							input.getOutputFields(), 
							new TriMapFunc(sop.getPlan(), sop.mapKeyType),
							output_fields
						).project(output_fields);
			} else if (sop.getType() == StormOper.OpType.BASIC_PERSIST || sop.getType() == StormOper.OpType.COMBINE_PERSIST) {
				// Group stuff.
				if (sop.getType() == StormOper.OpType.BASIC_PERSIST) {
					output = input.groupBy(new Fields(input.getOutputFields().get(0)))
							.persistentAggregate(
								sop.getStateFactory(pc),
								input.getOutputFields(),
								new CombineWrapper(new TriBasicPersist()), 
								output_fields
							).newValuesStream();
				} else {					
					// We need to trim things from the plan re:PigCombiner.java
					POPackage pack = (POPackage) sop.getPlan().getRoots().get(0);
					sop.getPlan().remove(pack);
					
					// Group
					GroupedStream gr_output = input.groupBy(new Fields(input.getOutputFields().get(0)));
					
					// Aggregate
					output = gr_output
							.persistentAggregate(
								sop.getStateFactory(pc),
								gr_output.getOutputFields(),
								new TriCombinePersist(pack, sop.getPlan(), sop.mapKeyType), 
								new Fields(output_fields.get(0) + "_tmp")
							).newValuesStream();

					// Decode the state variable to tuples.
					output = output.each(
								output.getOutputFields(), 
								new TriCombinePersist.StateClean(), 
								output_fields
							).project(new Fields(input.getOutputFields().get(0), output_fields.get(0)));
				}
			} else if (sop.getType() == StormOper.OpType.REDUCE_DELTA) {
				// Need to reduce
				output = input.each(
							input.getOutputFields(), 
							new TriReduce(sop.getPlan()), 
							output_fields
						).project(output_fields);
				output.each(output.getOutputFields(), new Debug());
			}
			
			sop_streams.put(sop, output);
			
//			System.out.println("input fields: " + input.getOutputFields());
//			System.out.println("output fields: " + output.getOutputFields());
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
