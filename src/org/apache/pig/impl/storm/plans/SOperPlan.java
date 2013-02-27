package org.apache.pig.impl.storm.plans;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.VisitorException;

public class SOperPlan extends OperatorPlan<StormOper> {
	
	public Set<String> UDFs = new HashSet<String>();
	public Map<POLoad, StormOper> PLSpoutLink = new HashMap<POLoad, StormOper>();
	
	/* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        SPrinter printer = new SPrinter(ps, this, null);
        printer.setVerbose(true);
        try {
            printer.visit();
        } catch (VisitorException e) {
            // TODO Auto-generated catch block
            throw new RuntimeException("Unable to get String representation of plan:" + e );
        }
        return baos.toString();
    }

	public void addPLSpoutLink(StormOper spout, POLoad pl) {
		// Note, this doesn't survive a clone right now...
		PLSpoutLink.put(pl, spout);
	}
	
	public StormOper getPLSpoutLink(POLoad pl) {
		return PLSpoutLink.get(pl);
	}
}
