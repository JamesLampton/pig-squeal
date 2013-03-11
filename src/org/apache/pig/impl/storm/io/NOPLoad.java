package org.apache.pig.impl.storm.io;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;

public class NOPLoad extends POLoad {

	public NOPLoad(OperatorKey k, POLoad load) {
		super(k);
		this.setLFile(load.getLFile());
		this.setAlias(load.getAlias());
	}

	@Override
    public Result getNext(Tuple t) throws ExecException {
		Result res = new Result();
		res.returnStatus = POStatus.STATUS_EOP;
		return res;
	}
	
	@Override
    public String name() {
        return (getLFile() != null) ? getAliasString() + "NOPLoad" + "(" + getLFile().toString()
                + ")" + " - " + mKey.toString() : getAliasString() + "Load" + "("
                + "DummyFil:DummyLdr" + ")" + " - " + mKey.toString();
    }
}
