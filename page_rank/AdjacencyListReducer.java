package first;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AdjacencyListReducer extends Reducer<Text, Text, NullWritable, GraphNode> {

	/* There is one reducer for each user, it receives as values the
	 * adjacency list for that user.
	 * It produces a GraphNode
	 */
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		Iterator<Text> it = values.iterator();
		
		String adjacencyList = "";
		while(it.hasNext()){
			// serialize each new value
			adjacencyList += it.next().toString() + ", ";
		}
		// it uses the substring of adjacencyList to eliminate the last ', '
		GraphNode node = new GraphNode(key, new Text(adjacencyList.substring(0, adjacencyList.length() - 2)),
				new FloatWritable(1));
		
		// write the result
		context.write(NullWritable.get(), node);

	}
}
