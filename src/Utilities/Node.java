package Utilities;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Node implements Serializable{
	Map<String, Node> children;
	Node parent = null;
	public String name="";
	int primaryChunkserverID = -1;
	long primaryLeaseIssueTime = -1;
	
	public ArrayList<Integer> chunkServersNum;
	
	public Node(){
		chunkServersNum = new ArrayList<Integer>();
		children = new HashMap<String, Node>();
	}
	
	//i should be 1 always!
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
	
	public void issuePrimaryLease(int CSID, long time){
		primaryChunkserverID = CSID;
		primaryLeaseIssueTime = time;
	}
	
	public int getPrimaryChunkserver(){
		return primaryChunkserverID;
	}
	
	public long getPrimaryLeaseTime(){
		return primaryLeaseIssueTime;
	}
}
