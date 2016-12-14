package first;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, GraphNode, NullWritable, GraphNode> {

	@Override
	public void reduce(Text key, Iterable<GraphNode> values, Context context) throws IOException, InterruptedException {
		GraphNode M = null;
		
		// get the global threshold 'alpha'
		// a node is stable when the delta for its pageRank is less than alpha
		float alpha = Float.parseFloat(context.getConfiguration().get("alpha"));
		
		Iterator<GraphNode> it = values.iterator();
		float partialSum = 0;
		
		while (it.hasNext()) {
			GraphNode nextNode = it.next();
	
			// retrieve the graph structure
			if (nextNode.getNodeName().equals(key))
				M = new GraphNode(nextNode);
			// or add the incoming pageRank mass
			else 
				partialSum += nextNode.getPageRank().get();
		}
		
		// if M is null, the value with its structure is missing
		if (M == null) {
			System.err.println("Graph structure missing for the node: " + key.toString());
			return;
		}
		
		// check the the delta between the old and the new pageRank
		// and signal this information with a Counter
		if (Math.abs(M.getPageRank().get() - partialSum) < alpha)
			context.getCounter(PageRankConvergence.STABLE).increment(1);
		else
			context.getCounter(PageRankConvergence.UNSTABLE).increment(1);
		
		// set the received as new pageRank
		M.setPageRank(partialSum);
		context.write(NullWritable.get(), M);
	}
}
