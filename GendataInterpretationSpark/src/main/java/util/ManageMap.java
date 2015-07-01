package util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class ManageMap {
	
	//private static final long serialVersionUID = 1L;
	
	public ArrayList<Object> getStringLongMax(HashMap<String, Long> mapStringLong) {
		ArrayList<Object> contain = new ArrayList<Object>();
		long max = 0;
		String maxCurrent = null;
		//almeno uno ci deve stare
		for(String key : mapStringLong.keySet()) {
			if (mapStringLong.get(key)>max) {
				max = mapStringLong.get(key);
				maxCurrent = key;
			}
		}
		contain.add(maxCurrent);
		contain.add(max);
		return contain;
	}
	
}
