/*
 * global counters for this MR job
 * 
 * When the NUMBER_OF_GRAY_NODES_PROCESSED == NUMBER_OF_GRAY_NODES_TOBE_PROCESSED, then we are completed
 * 
 */
public enum MRStats {
	NUMBER_OF_GRAY_NODES_TOBE_PROCESSED,  // used to keep track of how many Gray nodes that we have to process
	NUMBER_OF_GRAY_NODES_PROCESSED // Number of Gray nodes that have been processed
}
