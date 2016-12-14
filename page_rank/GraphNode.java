package first;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*;

/*
 * Class GraphNode represents a node element for a graph representation
 * to be used also as key/value in MapReduce.
 * It contains: identifier of the node, adjacency list (serialized) and the PageRank.
 */
public class GraphNode implements WritableComparable<GraphNode>{
	private Text adjacencyList;
	private Text nodeName;
	private FloatWritable pageRank;
	
	public GraphNode(){
		set(new Text(), new Text(), new FloatWritable());
	}
	
	public GraphNode(GraphNode o){
		set(new Text(o.getNodeName().toString()), 
				new Text(o.getAdjacencyList().toString()), 
						new FloatWritable(o.getPageRank().get()));
	}
	
	public GraphNode(String nodeName, String adjacencyList, float pageRank){
		set(new Text(nodeName), new Text(adjacencyList), new FloatWritable(pageRank));
	}
	
	public GraphNode(Text nodeName, Text adjacencyList, FloatWritable pageRank){
		set(nodeName, adjacencyList, pageRank);
	}
	
	public void set(Text nodeName, Text adjacencyList, FloatWritable pageRank){
		this.nodeName = nodeName;
		this.adjacencyList = adjacencyList;
		this.pageRank = pageRank;
	}
	
	public void set(String nodeName, String adjacencyList, float pageRank){
		this.nodeName = new Text(nodeName);
		this.adjacencyList = new Text(adjacencyList);
		this.pageRank = new FloatWritable(pageRank);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		nodeName.readFields(in);
		adjacencyList.readFields(in);
		pageRank.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		nodeName.write(out);
		adjacencyList.write(out);
		pageRank.write(out);
	}
	
	@Override
	public int hashCode(){
		return nodeName.hashCode() * 163 + adjacencyList.hashCode()*19 
				+ pageRank.hashCode();
	}
	
	@Override
	public boolean equals(Object o){
		if (o instanceof GraphNode) {
			GraphNode tp = (GraphNode) o;
			return nodeName.equals(tp.nodeName) && adjacencyList.equals(tp.adjacencyList) 
					&& pageRank.equals(tp.pageRank);
		}
		return false;
	}

	@Override
	public int compareTo(GraphNode o) {
		return nodeName.compareTo(o.nodeName);
	}

	@Override
	public String toString() {
		return String.format("(node_id: %s) (pageRank: %f) (neighbours: %s)",
				nodeName.toString(), pageRank.get(), adjacencyList.toString());
	}
	
	public Text getNodeName() {
		return nodeName;
	}
	
	public FloatWritable getPageRank() {
		return pageRank;
	}
	
	public Text getAdjacencyList() {
		return adjacencyList;
	}
	
	public void setPageRank(float pr) {
		this.pageRank = new FloatWritable(pr);
	}


}
