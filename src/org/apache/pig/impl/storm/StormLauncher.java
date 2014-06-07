package org.apache.pig.impl.storm;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Map.Entry;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.ExecType;
import org.apache.pig.PigRunner.ReturnCode;
import org.apache.pig.backend.BackendException;
import org.apache.pig.backend.datastorage.ContainerDescriptor;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.Launcher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.DotMRPrinter;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MRPrinter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.storm.plans.MRtoSConverter;
import org.apache.pig.impl.storm.plans.ReplJoinFileFixer;
import org.apache.pig.impl.storm.plans.ReplJoinFixer;
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
    class NoCompileMapReduceLauncher extends MapReduceLauncher {
    	private MROperPlan preCompiledPlan;

		public NoCompileMapReduceLauncher(MROperPlan preCompiledPlan) {
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
			Exception {

		log.trace("Entering StormLauncher.launchPig");

		// Now compile the plan into a Storm plan.
		SOperPlan sp = compile(php, pc);
		
		// If there is a static portion portion, execute it now.
		if (!pc.getProperties().getProperty("pig.streaming.no.static", "false").equalsIgnoreCase("true") && sp.getStaticPlan() != null) {
			log.info("Launching Hadoop jobs to perform static calculations...");
			NoCompileMapReduceLauncher mrlauncher = new NoCompileMapReduceLauncher(sp.getStaticPlan());
			PigStats ps = mrlauncher.launchPig(php, grpName, pc);
			if (ps.getReturnCode() != ReturnCode.SUCCESS) {
				log.warn("Ran into issues running static portion of job, aborting.");
				return ps;
			}
			
			// For replicated join files:
			// The temp files will be deleted by Pig's Main.  The topology will live on
			// beyond this window, so we will move all the temp files to a new location.
			if (sp.getReplFileMap() != null) {
				DataStorage dfs = pc.getDfs();

				// Make the base directory in the ugliest way possible.
				FileSpec a_spec = (FileSpec) sp.getReplFileMap().values().toArray()[0];
				ElementDescriptor tmp = dfs.asElement(a_spec.getFileName());
				OutputStream tmp_fh = tmp.create();
				tmp_fh.close();
				tmp.delete();

				for (Entry<FileSpec, FileSpec> ent : sp.getReplFileMap().entrySet()) {
					log.info("Moving " + ent.getKey() + " to " + ent.getValue());
					ElementDescriptor fn_from = dfs.asElement(ent.getKey().getFileName());
					ElementDescriptor fn_to = dfs.asElement(ent.getValue().getFileName());
					fn_from.rename(fn_to);
				}	
			}
		}
		
		if (sp.getReplFileMap() != null) {
			// Alter the plan to load from the new locations.
			new ReplJoinFileFixer(sp).convert();
		}

		if (pc.getProperties().getProperty("pig.streaming.topology.name", null) == null) {
			pc.getProperties().setProperty("pig.streaming.topology.name", "PigStorm-" + php.getLeaves().get(0).getAlias());
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
			// TODO: Execute "storm jar blah.jar";
			
			
			
			// For testing purposes.
			log.info("Setting up the topology runner...");
			Main m = new Main(pc, sp);
			log.info("Launching!");
//			m.launch();
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
		
		ps.println();
		
		// Now compile the plan into a Storm plan and explain.
		SOperPlan sp = compile(pp, pc);
	
        if (format.equals("text")) {
    		if (sp.getStaticPlan() != null) {
    			ps.println("#--------------------------------------------------");
                ps.println("# Storm Plan -- Static MR Portion                  ");
                ps.println("#--------------------------------------------------");
                
                MRPrinter mrprinter = new MRPrinter(ps, sp.getStaticPlan());
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
		
		MRtoSConverter converter = new MRtoSConverter(mrp, pc);
		converter.convert();
		
		return converter.getSPlan();
	}

	@Override
	public void kill() throws BackendException {
		// TODO Auto-generated method stub
		// Not really necessary for a streaming job...
	}

	@Override
	public void killJob(String jobID, Configuration conf)
			throws BackendException {
		// TODO Auto-generated method stub
		// Not really necessary for a streaming job...
	}
}
