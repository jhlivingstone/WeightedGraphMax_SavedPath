import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;



  /**
   * This is the Mapper Class
   * 
   * Nodes that are Color.WHITE or Color.BLACK are emitted, as is. For every
   * edge of a Color.GRAY node, we emit a new Node with distance incremented by
   * one. The Color.GRAY node is then colored black and is also emitted.
   * 
   * Key: File Offset   
   * Value: a string that has the following format: ID <tab> WEIGHT|EDGES|DISTANCE|COLOR|path_taken_edges|
   */
  @SuppressWarnings("deprecation")
public class WeightedGraphMaxSearchMapper extends MapReduceBase implements
      Mapper<LongWritable, Text, IntWritable, Text> {
	  
		private static final Logger LOG = Logger.getLogger(WeightedGraphMaxSearchMapper.class);

    public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output,
        Reporter reporter) throws IOException {

      LOG.info("Map executing for input key file offset = " + key.toString() + " and value:  " + value.toString());

      WeightedNode node = new WeightedNode(value.toString());

      // For each GRAY node, emit each of the edges as a new node (also GRAY)
      if (node.getColor() == WeightedNode.Color.GRAY) {
        for (int v : node.getEdges()) {
          WeightedNode vnode = new WeightedNode(v);
          // set weight to zero - indicating that we do not know the weight of the node yet
          // The weight will be set during the reduce phase.
          vnode.setWeight(0);
          // distance to new grey node = distance to parent node + parent node's weight
          vnode.setDistance(node.getDistance() + node.getWeight());
          // Set the color to Gray so that we will operate on this node during the next iteration of MapReduce 
          vnode.setColor(WeightedNode.Color.GRAY);
          // Increment the number of Gray nodes that we have to process...
          reporter.incrCounter(MRStats.NUMBER_OF_GRAY_NODES_TOBE_PROCESSED, 1);
          /*
           *  Add the Node ID in the path_taken_edges that so that we keep track of how we got to this node (who changed it to Gray)
           */
          // First save how we got to the parent node (deep copy)...
          for (int pathTakenEdge : node.getPathTakenEdges())
          {
              vnode.addPathTakenEdge(pathTakenEdge);
          }
          // Then append the parent node ID to the list...
          vnode.addPathTakenEdge(node.getId());

          // Emit gray node for each edge.
          output.collect(new IntWritable(vnode.getId()), vnode.getLine());
          LOG.info("Map output for key = " + vnode.getId() + " and value:  " + vnode.getLine());
       }
        // We're done with this node now, color it BLACK
        node.setColor(WeightedNode.Color.BLACK);
        /*
         *  Since this node was Gray and now we have changed it to Black, 
         *  then increment the number of Gray nodes that we have processed...
         */
        reporter.incrCounter(MRStats.NUMBER_OF_GRAY_NODES_PROCESSED, 1);

      }

      // No matter what, we emit the input node
      // If the node came into this method GRAY, it will be output as BLACK
      output.collect(new IntWritable(node.getId()), node.getLine());

      LOG.info("Map output for key = " + node.getId() + " and value:  " + node.getLine());

    }
  }

