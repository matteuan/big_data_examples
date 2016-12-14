package exercise3;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

/*
 * UDF for filtering out the list of tuples that contain cycles.
 */
public class CycleFree extends FilterFunc {

	@Override
	public Boolean exec(Tuple t) throws IOException {
		if(t == null || t.size() == 0)
			return false;
		// get all the fields in the tuple
		List<Object> items = t.getAll();
		HashMap<String, Boolean> itemPresent = new HashMap<String, Boolean>();
		for (Object o : items){
			String item = o.toString();
			// if there is a repetition -> cycle
			if (itemPresent.containsKey(item)) return false;
			else
				itemPresent.put(item, true);
		}
		return true;
	}

}
