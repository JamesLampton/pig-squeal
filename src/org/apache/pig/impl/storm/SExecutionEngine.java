package org.apache.pig.impl.storm;

import org.apache.pig.ExecType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
import org.apache.pig.impl.PigContext;

public class SExecutionEngine extends HExecutionEngine {

	public SExecutionEngine(PigContext pigContext) {
		super(pigContext);
		// TODO Auto-generated constructor stub
	}

	public void init() throws ExecException {
		// Pull the pig ExecType and change it to fool HExecutionEngine.
		ExecType memo = pigContext.getExecType();
		if (pigContext.getExecType() == ExecType.STORM) {
			pigContext.setExecType(ExecType.MAPREDUCE);
		} else if (pigContext.getExecType() == ExecType.STORMLOCAL) {
			pigContext.setExecType(ExecType.LOCAL);
		}
        super.init();
        // Restore it if necessary.
        if (pigContext.getExecType() != memo) {
        	pigContext.setExecType(memo);
        }
    }
}
