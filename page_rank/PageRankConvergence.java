package first;

/*
 *  this enum is used within counters to signal when the pageRank value
 *  for a node is stable between iterations or not.
 */
public enum PageRankConvergence {
	STABLE,
	UNSTABLE
}
