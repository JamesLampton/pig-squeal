package org.apache.pig.impl.storm.state;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Map;

import org.apache.pig.impl.PigContext;
import org.mortbay.util.ajax.JSON;

import storm.trident.state.StateFactory;
import storm.trident.testing.LRUMemoryMapState;

public class StateWrapper {
	private String stateFactoryCN;
	private Object[] args;
	private String staticMethod;
	private String jsonOpts;
	
	public StateWrapper(String jsonOpts) {
		this.jsonOpts = jsonOpts;
		
		// Decode jsonOpts
		if (jsonOpts != null) {
			Map<String, Object> m = (Map<String, Object>) JSON.parse(jsonOpts);

			stateFactoryCN = (String) m.get("StateFactory");
			args = (Object[]) m.get("args");
			staticMethod = (String) m.get("StaticMethod");
		}
//		this.stateFactoryCN = stateFactoryCN;
//		this.staticMethod = staticMethod;
//		this.jsonArgs = jsonArgs;
	}
	
	public StateFactory getStateFactory() {
		if (stateFactoryCN == null) {
			return new LRUMemoryMapState.Factory(10000);
		}
		
		try {
			Class<?> cls = PigContext.getClassLoader().loadClass(stateFactoryCN);
			
			Class<?> cls_arr[] = null;
			if (args != null) {
				cls_arr = new Class<?>[args.length];
				for (int i = 0; i < args.length; i++) {
					cls_arr[i] = args[i].getClass();
				}
			}
			
			if (staticMethod != null) {
				Method m = cls.getMethod(staticMethod, cls_arr);
				return (StateFactory) m.invoke(cls, args);
			} else {			
				if (args != null) {
					Constructor<?> constr = cls.getConstructor(cls_arr);
					return (StateFactory) constr.newInstance(args);
				} else {
					return (StateFactory) cls.newInstance();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
}
