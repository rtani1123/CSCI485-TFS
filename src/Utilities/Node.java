package Utilities;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Node {
	Map<String, Node> children =new HashMap<>();
	Node parent = null;
	String name="";
	
	ArrayList<Integer> chunkServersNum = new ArrayList<>();
	
	Node find (ArrayList<String> paths, int i){
		if (paths.size() == i)
			return this;
		if (this.children.containsKey(paths.get(i)))
			return children.get(paths.get(i)).find(paths, i+1);
		return null;
	}
}
