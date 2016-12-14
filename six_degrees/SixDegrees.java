package exercise3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class SixDegrees {
	
	   public static void main(String[] args) throws ExecException, IOException {
	      
	      PigServer pigServer = new PigServer("local");
	      if (args.length != 4){
	    	  System.err.println("Usage: SixDegrees <StorageJar> <CyclesJar> <Input> <Output>");
	    	  return;
	      }
	      try {
	         pigServer.registerJar(args[0]);
	         pigServer.registerJar(args[1]);
	         sixDegreesProof(pigServer, args[2], args[3]);
	        } 
	      catch (IOException e) {
	         e.printStackTrace();
	        }
	   }
	   
	   public static void sixDegreesProof(PigServer pigServer, String inputFile, String outputFile) throws IOException {  
	       pigServer.registerQuery("indata = LOAD'" + inputFile + "' USING RDFStorage() AS (s,p,o);");
	       pigServer.registerQuery("knows = FILTER indata BY p MATCHES 'foaf:knows';");

	       pigServer.registerQuery("level0 = FOREACH knows GENERATE s,o;");
	       pigServer.registerQuery("level0bis = FOREACH knows GENERATE s,o;");
	       pigServer.registerQuery("cumulative = GROUP level0 BY ($0, $1);");

	       // count the distinct users, in order to calculate the number of expected pairs
	       pigServer.registerQuery("distinctUsers = DISTINCT (FOREACH level0 GENERATE $0);");
	       pigServer.registerQuery("usersCount = FOREACH (GROUP distinctUsers ALL) GENERATE COUNT(distinctUsers);"); 
	       Iterator<Tuple> t = pigServer.openIterator("usersCount");
	       int numberOfUsers = Integer.parseInt(t.next().get(0).toString());
	       int numberOfPairs = (numberOfUsers * (numberOfUsers - 1)) / 2;
	       boolean allPairsHavePath = false;
	       
	       // iterate 6 times only if the break condition is not reached before
	       for (int i = 1; i < 6; i++){
	    	   // Join the previous level with the initial friends list and 
	    	   // remove the repetition of the join attribute
	    	   pigServer.registerQuery(String.format(
	    			   "level%d = JOIN level%d BY $%d, level0bis BY $0 ;", i, i-1, i ));
	    	   pigServer.registerQuery(String.format(
	    			   "level%d = FOREACH level%d GENERATE $0..$%d, $%d ;", i, i, i, i+2));
	    	   
	    	   // Remove the paths that contain Cycles
	    	   pigServer.registerQuery(String.format(
	    			   "level%d = FILTER level%d BY exercise3.CyclePresent($0..$%d) ;", i, i, i+1));
	    	   
	    	   // group for each pair of users
	    	   pigServer.registerQuery(String.format(
	    			   "level%d = GROUP level%d BY ($0, $%d);", i, i, i +1)) ;
	    	   
	    	   // add to the final cumulative set the new paths
	    	   pigServer.registerQuery(String.format(
	    			   "cumulative = GROUP (UNION cumulative, level%d) BY group;", i));
	    	   
	    	   // get only one path for each pair in the cumulative
	    	   pigServer.registerQuery(
	    			   "cumulative = FOREACH cumulative {"
	    			   + "firstPath = LIMIT $1 1;"
	    			   + "GENERATE FLATTEN(firstPath); };");
	    	   pigServer.registerQuery("cumulative = FOREACH cumulative {"
	    	   		+ "firstPath = LIMIT $1 1;"
	    	   		+ "GENERATE $0, firstPath;};");
	    	   
	    	   // if I found a path for every pair, we found a positive result
	    	   pigServer.registerQuery("pairsCount = FOREACH (GROUP cumulative ALL) GENERATE COUNT(cumulative);");
	    	   Iterator<Tuple> pairsCountIt = pigServer.openIterator("pairsCount");
		       int pairsCount = Integer.parseInt(pairsCountIt.next().get(0).toString()) / 2;
		       if (pairsCount == numberOfPairs){
		    	   allPairsHavePath = true;
		    	   break;
		       }
	    	   
	    	   // update the actual level for the next cycle
	    	   pigServer.registerQuery(String.format(
	    			   "level%d = FOREACH level%d {"
	    			   + "firstPath = LIMIT $1 1;"
	    			   + "GENERATE FLATTEN(firstPath); };", i, i, i));
	       }
	       // if we found that all the users
	       if (allPairsHavePath){
		       // remove the duplicate pairs e.g. only one of A->B and B->A
	    	   pigServer.registerQuery("cumulative = FILTER cumulative BY group.$0 < group.$1;");
		       pigServer.store("cumulative", outputFile);
	       } else {
	    	   System.out.println("Not every pair is connected within 6 steps.");
	       }
	   }
	   
	}