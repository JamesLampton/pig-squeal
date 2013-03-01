package org.apache.pig.impl.storm;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigRunner.ReturnCode;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobCreationException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.Launcher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.DotMRPrinter;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MRPrinter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.storm.plans.MRtoSConverter;
import org.apache.pig.impl.storm.plans.SOperPlan;
import org.apache.pig.impl.storm.plans.SPrinter;
import org.apache.pig.impl.util.JarManager;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.PigStatsUtil;

public class StormLauncher extends Launcher {
	public static final String PLANKEY = "__STORM_PLAN";
	
    private static final Log log = LogFactory.getLog(StormLauncher.class);

    // Yes, this is evil.
    class NullCompileMapReduceLauncher extends MapReduceLauncher {
    	private MROperPlan preCompiledPlan;

		public NullCompileMapReduceLauncher(MROperPlan preCompiledPlan) {
    		this.preCompiledPlan = preCompiledPlan;
    	}
    	
		@Override
		public MROperPlan compile(
	            PhysicalPlan php,
	            PigContext pc) throws PlanException, IOException, VisitorException {
			return preCompiledPlan;
		}
    }
    
	@Override
	public PigStats launchPig(PhysicalPlan php, String grpName, PigContext pc)
			throws PlanException, VisitorException, IOException, ExecException,
			JobCreationException, Exception {

		log.trace("Entering StormLauncher.launchPig");

		// Now compile the plan into a Storm plan.
		SOperPlan sp = compile(php, pc);
		
		// If there is a replicated join build portion, execute it now.
		if (sp.getReplPlan() != null) {
			log.info("Launching Hadoop jobs to build replicated join input...");
			NullCompileMapReduceLauncher mrlauncher = new NullCompileMapReduceLauncher(sp.getReplPlan());
			PigStats ps = mrlauncher.launchPig(php, grpName, pc);
			if (ps.getReturnCode() != ReturnCode.SUCCESS) {
				log.warn("Ran into issues building the replicated join files, aborting.");
				return ps;
			}
		}

		// Encode the plan into the context for later retrieval.
		log.info("Stashing the Storm plan into PigContext for retrieval by the topology runner...");
		pc.getProperties().setProperty(PLANKEY, ObjectSerializer.serialize(sp));
		
		// Build the jar file.
		if (!pc.inIllustrator) 
        {
			File submitJarFile;
			if (pc.getProperties().getProperty("pig.streaming.jarfile", null) != null) {
				submitJarFile = new File(pc.getProperties().getProperty("pig.streaming.jarfile"));
				log.info("creating jar from property: "+submitJarFile.getName());
			} else {
				//Create the jar of all functions and classes required
	            submitJarFile = File.createTempFile("Job", ".jar");
	            log.info("creating jar file "+submitJarFile.getName());
	            // ensure the job jar is deleted on exit
	            submitJarFile.deleteOnExit();
			}
            FileOutputStream fos = new FileOutputStream(submitJarFile);
            JarManager.createJar(fos, sp.UDFs, pc);
            
            log.info("jar file "+submitJarFile.getName()+" created");
        }
		
		// Remove the storm plan from the PC
		pc.getProperties().remove(PLANKEY);
		
		// Launch the storm task.
		try {
			log.info("Setting up the topology runner...");
			Main m = new Main(pc, sp);
			// TODO
			log.info("Launching!");
			m.launch();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
//		int ret = failed ? ((succJobs != null && succJobs.size() > 0) 
//				? ReturnCode.PARTIAL_FAILURE
//						: ReturnCode.FAILURE)
//						: ReturnCode.SUCCESS; 
		return PigStatsUtil.getPigStats(ReturnCode.SUCCESS);
	}
	
	@Override
	public void explain(PhysicalPlan pp, PigContext pc, PrintStream ps,
			String format, boolean verbose) throws PlanException,
			VisitorException, IOException {
		
		log.trace("Entering StormLauncher.explain");
		
		// TODO: Put this back.
		ps.println();
//		MapReduceLauncher mrlauncher = new MapReduceLauncher();
//		try {
//			mrlauncher.explain(pp.clone(), pc, ps, format, verbose);
//		} catch (CloneNotSupportedException e) {
//			e.printStackTrace();
//		}
		
		// Now compile the plan into a Storm plan and explain.
		SOperPlan sp = compile(pp, pc);


		
        if (format.equals("text")) {
    		if (sp.getReplPlan() != null) {
    			ps.println("#--------------------------------------------------");
                ps.println("# Storm Plan -- Replicated Join MR Portion         ");
                ps.println("#--------------------------------------------------");
                
                MRPrinter mrprinter = new MRPrinter(ps, sp.getReplPlan());
                mrprinter.setVerbose(verbose);
                mrprinter.visit();
    		}

    		SPrinter printer = new SPrinter(ps, sp, pc);
            printer.setVerbose(verbose);
            
            printer.visit();
        } else {
            ps.println("#--------------------------------------------------");
            ps.println("# Storm Plan                                       ");
            ps.println("#--------------------------------------------------");

            // TODO
//            DotMRPrinter printer =new DotMRPrinter(mrp, ps);
//            printer.setVerbose(verbose);
//            printer.dump();
//            ps.println("");
        }
		
	}

	public SOperPlan compile(PhysicalPlan php, PigContext pc) 
			throws PlanException, IOException, VisitorException {
		MapReduceLauncher mrlauncher = new MapReduceLauncher();
		MROperPlan mrp = mrlauncher.compile(php, pc);
		
		MRtoSConverter converter = new MRtoSConverter(mrp);
		converter.convert();
		
		return converter.getSPlan();
	}
}
