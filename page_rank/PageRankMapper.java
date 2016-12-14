package first;

import java.io.IOException;


import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<Object, Text, Text, GraphNode> {

	// RE for matching a GraphNode
	private Pattern nodeRE = Pattern.compile(
			"\\(node_id: (?<nodeName>\\S*)\\) "
			+ "\\(pageRank: (?<pageRank>\\S*)\\) "
			+ "\\(neighbours: (?<adjacencyList>.*)\\)");
	

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// parse the input
		GraphNode N = parseGraphInput(value.toString());
		// it encountered some problem while parsing
		if (N == null) {
			System.err.println("Received wrong input string: " + value.toString());
			return;
		}
		
		// pass along graph structure
		context.write(N.getNodeName(), N);
		
		String [] adjacencyList = N.getAdjacencyList().toString().split(", ");
		float p = N.getPageRank().get() / adjacencyList.length;
		
		// communicate pageRank to neighbours
		for (int i = 0; i < adjacencyList.length; i++){
			// use a graphNode to communicate to the other node the pageRank
			// it is distinguished from the other because the nodeName of the node
			// is different from the key
			GraphNode message = new GraphNode(N.getNodeName().toString(), "", p);
			context.write(new Text(adjacencyList[i]), message);
		}
	}
	
	/* parseGraphInput pars a string of the type:
	 * (node_id: <user>) (pageRank: <p>) (neighbours:<list>)
	 * returns a GraphNode filled with the parsed data */
	public GraphNode parseGraphInput(String in){
		Matcher m = nodeRE.matcher(in);
		if (!m.find()) return null;
		
		// get the groups from the RE
		GraphNode parsedNode = new GraphNode(
				m.group("nodeName"),
				m.group("adjacencyList"),
				Float.parseFloat(m.group("pageRank")));
		
		return parsedNode;
	}
}
