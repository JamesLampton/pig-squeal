package org.apache.pig.impl.storm.state;

import org.apache.hadoop.io.MapWritable;

public abstract class MapIdxWritable extends MapWritable implements IPigIdxState {

	public String toString() {
		return this.getClass().getSimpleName() + "@" + this.hashCode() + ": " + this.entrySet();
	}
}
