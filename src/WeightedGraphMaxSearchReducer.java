import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;


/**
 * A  reducer class that just emits the sum of the input values and the path that got us to that node.
 * 
 * Note that the reducer executes for each key (each Node ID)
 * 
 * Input key is the node ID
 * Input values have the following format: WEIGHT|EDGES|DISTANCE|COLOR|path_taken_edges|
 */
@SuppressWarnings("deprecation")
public class WeightedGraphMaxSearchReducer extends MapReduceBase implements
    Reducer<IntWritable, Text, IntWritable, Text> {

	private static final Logger LOG = Logger.getLogger(WeightedGraphMaxSearchReducer.class);


  public void reduce(IntWritable key, Iterator<Text> values,
      OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
  	
    LOG.info("Reduce executing for input key= " + key.toString());


    List<Integer> edges = null;
    // Contains the path taken to the node
	List<Integer> PathTakenEdges = new ArrayList<Integer>();
    int weight = 0;
    int distance = 0;
    WeightedNode.Color color = WeightedNode.Color.WHITE;
    // indicates the color of the maximum distance node that got us to the Node ID (key).  Initially set to White
    WeightedNode.Color maxDistanceWeightedNodeColor = WeightedNode.Color.WHITE;  

    // Loop through all of the values for this key (node ID)
    while (values.hasNext()) {

    	Text value = values.next();
    	LOG.info("Processing Value: " + value.toString());

      WeightedNode u = new WeightedNode(key.get() + "\t" + value.toString());

      // Save the maximum weight
      if (u.getWeight() > weight) {
      	weight = u.getWeight();
      }
      
      // Save the edges for the node, if they are specified...
      if (u.getEdges().size() > 0) {
        edges = u.getEdges();
      }

      // Save the maximum distance
      if (u.getDistance() > distance) {
    	  distance = u.getDistance();

    	  // For a maximum distance node, Gray or Black, save the path to that maximum distance node...
    	  if (u.getColor().ordinal() >=  WeightedNode.Color.GRAY.ordinal())
    	  {
    		  // Clear the PathTakenEdges...
    		  PathTakenEdges.clear();
    		  // do a deep copy of path taken edges information...
    		  for (int pathTakenEdge : u.getPathTakenEdges())
    		  {
    			  PathTakenEdges.add(pathTakenEdge);
    		  }
    	  }
    	  // save the color of the maximum distance node
    	  maxDistanceWeightedNodeColor = u.getColor();
      }

      /* 
       * If the existing color of the maximum distance node is white, 
       * and we happen across a Gray or Black node, 
       * then save the Path Taken information because
       * the GRAY nodes have the Path taken Edges information...
       */

      else if (
    		  (maxDistanceWeightedNodeColor == WeightedNode.Color.WHITE) &&
    		  (u.getColor().ordinal() >=  WeightedNode.Color.GRAY.ordinal())
      )
      {
    	  // Clear the PathTakenEdges...
    	  PathTakenEdges.clear();
    	  // do a deep copy of path taken edges information...
    	  for (int pathTakenEdge : u.getPathTakenEdges())
    	  {
    		  PathTakenEdges.add(pathTakenEdge);
    	  }
      }

      // Save the darkest color
      if (u.getColor().ordinal() > color.ordinal()) {
        color = u.getColor();
      }
      
    }

    WeightedNode n = new WeightedNode(key.get());
    n.setWeight(weight);
    n.setDistance(distance);
    n.setEdges(edges);
    n.setColor(color);
    if (!(PathTakenEdges == null))
    {
        n.setPathTakenEdges(PathTakenEdges);
    }
    
    // Emit the reduced node...
    output.collect(key, new Text(n.getLine()));
    LOG.info("Reduce output key= " + key + " and value: " + n.getLine());
  }
}
