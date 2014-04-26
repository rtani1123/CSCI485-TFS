package Utilities;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	
	//i should be 1 always!
	public Node find (List<String> tempPaths, int i){
		if (tempPaths.size() == i)
			return this;
		if (this.children.containsKey(tempPaths.get(i)))
			return children.get(tempPaths.get(i)).find(tempPaths, i+1);
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
