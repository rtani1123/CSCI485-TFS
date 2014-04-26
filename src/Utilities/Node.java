package Utilities;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * A node represents a unique chunkhanlde or file path in master namespace.
 * Each node has a map of children and a parent so that nodes would be connected to each other.
 * Also it maintains the primary leases and a name.
 * @author boghrati
 *
 */
public class Node implements Serializable{
	static Integer lock = -1;
	public Map<String, Node> children;
	Node parent = null;
	public String name="";
	int primaryChunkserverID = -1;
	long primaryLeaseIssueTime = -1;
	
	public List<Integer> chunkServersNum;
	
	public Node(){
		chunkServersNum = Collections.synchronizedList(new ArrayList<Integer>());
		children = Collections.synchronizedMap(new HashMap<String, Node>());
	}
	
	/**
	 * It's a recursive function to find a node with specfied path.
	 * When first calling this function, i should be passed as 1
	 * @param tempPaths
	 * @param i
	 * @return
	 */
	public Node find (List<String> tempPaths, int i){
		if (tempPaths.size() == i)
			return this;
		if (this.children.containsKey(tempPaths.get(i)))
			return children.get(tempPaths.get(i)).find(tempPaths, i+1);
		return null;
	}
	/**
	 * Returns the full path of a given node.
	 * @return
	 */
	public String getPath(){
		String fullPath="";
		if(this.parent!=null)
			fullPath = this.parent.getPath()+"/"+this.name;
		else 
			fullPath =this.name;
		return fullPath;
	}
	/**
	 * Sets the primary lease
	 * @param CSID
	 * @param time
	 */
	public void issuePrimaryLease(int CSID, long time){
		synchronized(lock) {
			primaryChunkserverID = CSID;
			primaryLeaseIssueTime = time;
		}
	}
	
	public int getPrimaryChunkserver(){
		return primaryChunkserverID;
	}
	
	public long getPrimaryLeaseTime(){
		return primaryLeaseIssueTime;
	}
}
