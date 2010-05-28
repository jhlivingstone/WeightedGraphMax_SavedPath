/**
 * Author: John Livingstone
 * 
 * This Hadoop (Using Cloudera Hadoop version: 0.20.1+152) MapReduce program operates on a directed graph, in adjacency list format.
 * The program computes the maximum total of node weights, from top to bottom of the directed graph, and records the path taken to
 * get to the maximum total node weight, by performing a breadth-first graph search using an iterative map-reduce algorithm. 
 * 
 * The directed graph is in the form of a triangle set of numbers.
 * This program takes as input a 'triangle' set of numbers in an adjacency list format.
 * The triangle of numbers represent a directed graph, where each node points to the 2 nodes directly below it on the next line.
 * For example, for the triangle below: (note that I add an 'aggregation node' with zero weight to the triangle as the last node)
 * 21
 * 10 13
 * 45 21 32
 * 0
 * 
 * Is represented by the following Adjacency List:
 * Node position 	Weight	Points to Nodes (Edges)
 * 1				21		2,3
 * 2				10		4,5
 * 3				13		5,6
 * 4				45		7
 * 5				21		7
 * 6				32		7
 * 7				0		
 * 
 * The input format is
 * ID   WEIGHT|EDGES|DISTANCE|COLOR
 * where
 * ID = the unique identifier for a node (assumed to be an int here)
 * WEIGHT = The value of the node - this integer value contributes to the 'distance' from the starting node
 * EDGES = the list of edges emanating from the node (e.g. 3,8,9,12)
 * DISTANCE = the Maximum 'to be determined' distance of the node from the source
 * COLOR = a simple status tracking field to keep track of when we're finished with a node
 * It assumes that the source node (the node from which to start the search) has
 * been marked with distance of zero and color GRAY in the original input.  All other
 * nodes will have input distance of zero and color WHITE.
 * 
 * An example line of input format file:

1	5|2,3,|0|GRAY|
2	10|4,5,|0|WHITE|
3	12|5,6,|0|WHITE|
4	17|7,8,|0|WHITE|
5	14|8,9,|0|WHITE|
6	15|9,10,|0|WHITE|
7	9|11,12,|0|WHITE|
8	14|12,13,|0|WHITE|
9	10|13,14,|0|WHITE|
10	6|14,15,|0|WHITE|
11	5|16,17,|0|WHITE|
12	4|17,18,|0|WHITE|
13	3|18,19,|0|WHITE|
14	2|19,20,|0|WHITE|
15	7|20,21,|0|WHITE|
16	6|22,|0|WHITE|
17	8|22,|0|WHITE|
18	5|22,|0|WHITE|
19	4|22,|0|WHITE|
20	3|22,|0|WHITE|
21	2|22,|0|WHITE|
22	0|23,|0|WHITE|
 * 
 * To run:   hadoop jar /home/training/workspace/Graph/bin/WeightedGraphMinSearch.jar -c IOFiles-HDFS-Config.xml
 * 
 * The output format is
 * ID   WEIGHT|EDGES|DISTANCE|COLOR|Path_taken_edges|
 * where
 * ID = the unique identifier for a node (assumed to be an int here)
 * WEIGHT = The value of the node - this integer value contributes to the 'distance' from the starting node
 * EDGES = the list of edges emanating from the node (e.g. 3,8,9,12)
 * DISTANCE = the Maximum 'to be determined' distance of the node from the source starting node
 * COLOR = a simple status tracking field to keep track of when we're finished with a node
 * Path_taken_edges: the nodes edges taken from the starting node to get to this node.
 * 
 * An example output file after the 6th iteration:
1	5|2,3,|0|BLACK||
2	10|4,5,|5|BLACK|1,|
3	12|5,6,|5|BLACK|1,|
4	17|7,8,|15|BLACK|1,2,|
5	14|8,9,|17|BLACK|1,3,|
6	15|9,10,|17|BLACK|1,3,|
7	9|11,12,|32|BLACK|1,2,4,|
8	14|12,13,|32|BLACK|1,2,4,|
9	10|13,14,|32|BLACK|1,3,6,|
10	6|14,15,|32|BLACK|1,3,6,|
11	5|16,17,|41|BLACK|1,2,4,7,|
12	4|17,18,|46|BLACK|1,2,4,8,|
13	3|18,19,|46|BLACK|1,2,4,8,|
14	2|19,20,|42|BLACK|1,3,6,9,|
15	7|20,21,|38|BLACK|1,3,6,10,|
16	6|22,|46|BLACK|1,2,4,7,11,|
17	8|22,|50|BLACK|1,2,4,8,12,|
18	5|22,|50|BLACK|1,2,4,8,12,|
19	4|22,|49|BLACK|1,2,4,8,13,|
20	3|22,|45|BLACK|1,3,6,10,15,|
21	2|22,|45|BLACK|1,3,6,10,15,|
22	0||58|BLACK|1,2,4,8,12,17,|
 * 
 */


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.log4j.Logger;


public class WeightedGraphMaxSearch extends Configured implements Tool {

	/*
	 * If the "-i" iteration parameter has been specified on the command line
	 * This contains the number of rows that have been asked to be processed...
	 */
	static int NumberOfRowsToProcess = 0;

	/*
	 *  Indicates when we want only one reducer (when set to true) so that all results are aggregated 
	 *  This is always set to one for the last iteration
	 */
	static boolean setOneReducer = false;


	/*
	 * These strings are used to specify the input and output files and directories
	 * Note that the output of iteration (i) is the input for iteration (i+1)
	 *  
	 *  Default values point to local HDFS
	 */
	static String inputPathFirstIteration = "input/input-graph"; 				// Path and filename of initial input graph
	static String inputPathOtherIterations = "output/outgraph/output-graph-";   // Path and partial filename of subsequent input graph (input for all other iterations)
	static String outputPathIteration = "output/outgraph/output-graph-";		// Path and partial filename of output graph 

	// Log4j...
	private static final Logger LOG = Logger.getLogger(WeightedGraphMaxSearch.class);

	
	/**
	 * Method to initialize the Job Configuration for each MR iteration.
	 * String[] args - the command line arguments
	 */
	@SuppressWarnings("deprecation")
	private JobConf getJobConf(String[] args) {

		String IO_Configuration_File = "";

		// instantiate a new Job Configuration object
		JobConf conf = new JobConf(getConf(), WeightedGraphMaxSearch.class);
		
		// Job Name..
		conf.setJobName("WeightedGraphMaxSearch");

		/*
		 * Set the output key and value class...
		 */
		// The keys are the unique identifiers for a Node (integers in this case).
		conf.setOutputKeyClass(IntWritable.class);
		// the values are the string representation of a Node
		conf.setOutputValueClass(Text.class);

		// Set the Mapper and Reducer classes...
		conf.setMapperClass(WeightedGraphMaxSearchMapper.class);
		conf.setReducerClass(WeightedGraphMaxSearchReducer.class);
		// Set combiner class, if we had defined one!
		// conf.setCombinerClass(theClass);

		/*
		 * From the command line parameters, retrieve the configuration file name (not optional) 
		 * and the (optional) 'asked for' number of iterations, mappers, and reducers
		 * For the Number of Mappers Note: This is only a hint to the framework. The actual number of 
		 * spawned map tasks depends on the number of InputSplits generated by the job's 
		 * InputFormat.getSplits(JobConf, int). A custom InputFormat is typically used to 
		 * accurately control the number of map tasks for the job.
		 */
		for (int i = 0; i < args.length; ++i) {

			// Set the configuration file that contains the Input and Output paths and filenames...
			if ("-c".equals(args[i])) {
				IO_Configuration_File =  args[++i];
				LOG.info("IO_Configuration_File: " + IO_Configuration_File);

				/*
				 * Set Input and Output paths...
				 */
				conf.addResource(IO_Configuration_File);
				inputPathFirstIteration = conf.get("inputPathFirstIteration");   // Path and filename of initial input graph
				inputPathOtherIterations = conf.get("inputPathOtherIterations"); // Path and partial filename of subsequent input graph (input for all other iterations)
				outputPathIteration = conf.get("outputPathIteration");           // Path and partial filename of output graph 
			} 

			// Set number of mappers 'hint' to framework...
			if ("-m".equals(args[i])) {
				conf.setNumMapTasks(Integer.parseInt(args[++i]));
				LOG.info("Number of Mappers (hint): " + conf.getNumMapTasks());
			} 

			// Set number of reducers...
			if ("-r".equals(args[i])) {
				if (setOneReducer)
				{
					// For the last iteration, Always consolidate the results by setting the number of reducers to one...
					conf.setNumReduceTasks(1);	
				}
				else
				{
					conf.setNumReduceTasks(Integer.parseInt(args[++i]));
				}
				LOG.info("Number of Reducers: " + conf.getNumReduceTasks());

			}

			/*
			 *  Set the total number of iterations of the MR job. 
			 *  For the 'triangle of numbers', this equates to the number of rows in the triangle
			 */
			if ("-i".equals(args[i])) {
				// Found iterator argument
				NumberOfRowsToProcess = Integer.parseInt(args[++i]);
				LOG.info("Total number of Iterations: " + NumberOfRowsToProcess);
			}

		}

		return conf;
	}




	/**
	 * The main driver for word count map/reduce program. 
	 * Invoke this method to submit the map/reduce job.
	 * 
	 * The MapReduce progr/tmp/VMwareDnD/803ce168/01 Un Petit Probleme.m4aam is run until there are no remaining Gray nodes
	 * 
	 * @throws IOException
	 *           When there is communication problems with the job tracker.
	 */
	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {

		int iterationCount = 0;
		
		// Assume that we have at least one Gray node to process...
		long numGrayNodesToBeProcessed = 1;
		long numGrayNodesProcessed = 0;

		// retrieve start Time... Get current time
		long start = System.currentTimeMillis();

		/*
		 * Continue looping if either of these conditions are true...
		 * 1. The total number of iterations was not specified via the -i command line parameter:
		 *    then loop until we have no more Gray nodes to process.
		 * 2. The number of iterations was specified via the -i command line parameter, so iterate only those number of times.
		 */
		while (
				(numGrayNodesToBeProcessed != 0) &&  // (1.)
				(
					(NumberOfRowsToProcess == 0) ||   // (1.)
					((NumberOfRowsToProcess > 0)  && (iterationCount < NumberOfRowsToProcess))  // (2.)
				)
		)
		{

			String input_filepath;

			// Retrieve the Job configuration...
			JobConf conf = getJobConf(args);


			if (iterationCount == 0)
			{
				input_filepath = inputPathFirstIteration;
			}
			else
			{
				input_filepath = inputPathOtherIterations + iterationCount;
			}

			String output_filepath = outputPathIteration + (iterationCount + 1);


			LOG.info("** Interation Count= " + iterationCount + " Input= " + input_filepath + " Output= " + output_filepath);
			FileInputFormat.setInputPaths(conf, new Path(input_filepath));
			FileOutputFormat.setOutputPath(conf, new Path(output_filepath));

			RunningJob job = JobClient.runJob(conf);

			Counters counters = job.getCounters();

			// Loop unitl the number of Gray nodes is zero...
			numGrayNodesToBeProcessed  = counters.getCounter(MRStats.NUMBER_OF_GRAY_NODES_TOBE_PROCESSED);
			numGrayNodesProcessed  = counters.getCounter(MRStats.NUMBER_OF_GRAY_NODES_PROCESSED);
			LOG.info("numGrayNodesProcessed Count= " + numGrayNodesProcessed);
			LOG.info("numGrayNodesToBeProcessed Count= " + numGrayNodesToBeProcessed);


			if (numGrayNodesToBeProcessed == 0)
			{
				LOG.info("End of Interations!  No Gray Nodes Left to Process!!!");
				// Get elapsed time in milliseconds
				long elapsedTimeMillis = System.currentTimeMillis()-start;
				// Get elapsed time in seconds
				long elapsedMin = elapsedTimeMillis / 60000;
				long elapsedSec = (elapsedTimeMillis % 60000) / 1000;
				float elapsedTimeSec = elapsedTimeMillis/1000F;


				System.out.println("MR Job Total Elapsed Time: "+ elapsedMin + " min, " + elapsedSec + " sec");
				System.out.println("MR Job Total Elapsed Time (sec): "+ elapsedTimeSec);

			}

			iterationCount++;
			
			/*
			 * Check if we are at the last iteration, if so - then 
			 * set the flag so that we only have one reducer for this iteration.
			 * With one reducer, all results are aggregated into part00000 and we will get one answer for the maximum path
			 */
			if (
					((NumberOfRowsToProcess == 0) && (numGrayNodesProcessed == numGrayNodesToBeProcessed)) ||
					((NumberOfRowsToProcess > 0)  && (iterationCount == (NumberOfRowsToProcess-1))) 
			)
			{
				LOG.info("Last Interation!  Set reducer flag to one!");
				setOneReducer = true;
			}
			
		}


		return 0;
	}



	public static void main(String[] args) throws Exception {

		boolean foundConfigurationArg = false;

		//need at least 2 arguments for valid call and one must be the configuration file... 
		if (args.length >= 2) 
		{
			for (int i = 0; i < args.length; ++i) {
				if ("-c".equals(args[i])) {
					foundConfigurationArg = true;
				}
			}
		}

		if (!foundConfigurationArg)
		{
			System.out.println("Usage: WeightedGraphMinSearch -c <Input / Output Configuration.xml file>");
			System.out.println("Optional Parameters are:");
			System.out.println(" -i <Number of Iterations>");
			System.out.println(" -m <Number of Map Tasks>");
			System.out.println(" -r <Number of Reduce Tasks>");
			System.out.println("where <Number of Iterations> is equal to the number of rows to be processed in the triangle");
			return;
		}



		int res = ToolRunner.run(new Configuration(), new WeightedGraphMaxSearch(), args);
		System.exit(res);
	}

}
