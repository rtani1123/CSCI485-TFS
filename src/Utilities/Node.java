package Utilities;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Node implements Serializable{
	Map<String, Node> children =new HashMap<>();
	Node parent = null;
	public String name="";
	
	public ArrayList<Integer> chunkServersNum = new ArrayList<>();
	
	public Node find (ArrayList<String> paths, int i){
		if (paths.size() == i)
			return this;
		if (this.children.containsKey(paths.get(i)))
			return children.get(paths.get(i)).find(paths, i+1);
		return null;
	}
	public String getPath(){
		String fullPath="";
		if(this.parent!=null)
			fullPath = this.parent.getPath()+"/"+this.name;
		else 
			fullPath =this.name;
		return fullPath;
	}
}
