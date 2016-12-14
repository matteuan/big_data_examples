package first;

import java.io.IOException;


import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AdjacencyListMapper extends Mapper<Object, Text, Text, Text> {

	// RE for matching a triple
	private Pattern tripleRE = Pattern.compile(
			"((?=[\"'])[\"'](?<subjectQuoted>.+)[\"']|(?<subject>\\S+))\\s"
			+ "((?=[\"'])[\"'](?<predicateQuoted>.+)[\"']|(?<predicate>\\S+))\\s"
			+ "((?=[\"'])[\"'](?<objectQuoted>.+)[\"']|(?<object>\\S+))");
	
	/*
	 * A single map instance receives a triple, filter only the triples
	 * of the type 'foaf:knows' and produces a single edge.
	 */
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		// match the triple with the regular expression
		Matcher m = tripleRE.matcher(value.toString());
		// if it is not a valid triple, ignore it 
		if (!m.find()) return;
		
		// extract from the triple: subject, predicate and object. the string could be quoted
		String triple_sbj = m.group("subjectQuoted") != null ? 
				m.group("subjectQuoted"): m.group("subject");
		String triple_pred = m.group("predicateQuoted") != null ? 
				m.group("predicateQuoted"): m.group("predicate");
		String triple_obj = m.group("objectQuoted") != null ? 
				m.group("objectQuoted"): m.group("object");

		// consider only 'foaf:knows' edges
		if (triple_pred.equals("foaf:knows")){
			// the key is the subject and the value the object
			Text out_key = new Text(triple_sbj);
			Text out_value =  new Text(triple_obj);
			
			context.write(out_key, out_value);
		} 
	}
}
