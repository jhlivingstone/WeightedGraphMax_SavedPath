
/*
 * The input format is
 * ID   WEIGHT|EDGES|DISTANCE|COLOR|Path_Taken_EDGES|
 * where
 * ID = the unique identifier for a node (assumed to be an int here)
 * WEIGHT = The value of the node - this integer value contributes to the 'distance' from the starting node
 * EDGES = the list of edges emanating from the node (e.g. 3,8,9,12)
 * DISTANCE = the to be determined distance of the node from the source
 * COLOR = a simple status tracking field to keep track of when we're finished with a node
 * It assumes that the source node (the node from which to start the search) has
 * been marked with distance 0 and color GRAY in the original input.  All other
 * nodes will have input distance Integer.MAX_VALUE and color WHITE.
 * 
 * An example input format file:

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
22	0||0|WHITE|

 */


import java.util.*;

import org.apache.hadoop.io.Text;

/*
 * ID   WEIGHT|EDGES|DISTANCE|COLOR
 */

public class WeightedNode {

	public static enum Color {
		WHITE, GRAY, BLACK
	};

	private final int id;
	private int weight;
	private int distance;
	private List<Integer> edges = new ArrayList<Integer>();
	private Color color = Color.WHITE;
	private List<Integer> path_taken_edges = new ArrayList<Integer>();

	public WeightedNode(String str) {

		// Split the input string into Key and Value...
		String[] map = str.split("\t");
		String key = map[0]; 
		String value = map[1];

		// Split value into tokens: WEIGHT|EDGES|DISTANCE|COLOR|Path_Taken_Edges
		String[] tokens = value.split("\\|");

		// Save node's key
		this.id = Integer.parseInt(key);

		// Weight tokens[0]...
		if (tokens[0].equals("Integer.MAX_VALUE")) {
			this.weight = Integer.MAX_VALUE;
		} else {
			this.weight = Integer.parseInt(tokens[0]);
		}
		
		// Edges tokens[1]...
		for (String s : tokens[1].split(",")) {
			if (s.length() > 0) {
				edges.add(Integer.parseInt(s));
			}
		}

		// Distance from origin tokens[2]...
		if (tokens[2].equals("Integer.MAX_VALUE")) {
			this.distance = Integer.MAX_VALUE;
		} else {
			this.distance = Integer.parseInt(tokens[2]);
		}

		// Color tokens[3]...
		this.color = Color.valueOf(tokens[3]);
		
		// Path Taken Edges tokens[4]...
		if (tokens.length >= 5){
			for (String s : tokens[4].split(",")) {
				if (s.length() > 0) {
					path_taken_edges.add(Integer.parseInt(s));
				}
			}
		}

	}

	public WeightedNode(int id) {
		this.id = id;
	}

	public int getId() {
		return this.id;
	}
	
	public int getWeight() {
		return this.weight;
	}

	public void setWeight(int distance) {
		this.weight = distance;
	}
	
	public int getDistance() {
		return this.distance;
	}

	public void setDistance(int distance) {
		this.distance = distance;
	}

	public Color getColor() {
		return this.color;
	}

	public void setColor(Color color) {
		this.color = color;
	}

	public List<Integer> getEdges() {
		return this.edges;
	}

	public void setEdges(List<Integer> edges) {
		this.edges = edges;
	}

	public List<Integer> getPathTakenEdges() {
		return this.path_taken_edges;
	}

	public void setPathTakenEdges(List<Integer> path_taken_edges) {
		this.path_taken_edges = path_taken_edges;
	}

	public void addPathTakenEdge(Integer path_taken_edges) {
		this.path_taken_edges.add(path_taken_edges);
	}
	
	/*
	 * This returns a string of the following form:  WEIGHT|EDGES|DISTANCE|COLOR|Path Taken Edges
	 */
	public Text getLine() {
		StringBuffer s = new StringBuffer();

		s.append(this.weight).append("|");

		// Check if this node has any edges - ie: check if it points to any other nodes...
		if ((edges != null) && (!(edges.isEmpty())))
		{
			for (int v : edges) {
				s.append(v).append(",");
			}
			
		}
		s.append("|");

		if (this.distance < Integer.MAX_VALUE) {
			s.append(this.distance).append("|");
		} else {
			s.append("Integer.MAX_VALUE").append("|");
		}

		s.append(color.toString());
		s.append("|");
		
		for (int v : path_taken_edges) {
			s.append(v).append(",");
		}
		s.append("|");


		return new Text(s.toString());
	}

}
