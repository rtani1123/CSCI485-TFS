package Utilities;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
/**
 * Store the master namespace in a tree structure. The root is hardcoded to be C:/ (since we're not allowed to create a new driver!)
 * Each tree consists of several nodes and each node represents a unique fullPath (directory or file)
 */
public class Tree implements Serializable {
	public Node root;
	public static void main(String[] args) {
		List<String> paths = Collections.synchronizedList(new ArrayList<String>());
		Tree myTree= new Tree();
		Storage ts = new Storage();
		myTree = ts.getTree();
		myTree.getAllPath(myTree.root);
	}
	/**
	 * Tokenizes a path string to a list of string.
	 * @param path
	 * @return
	 */
	public static List<String> pathTokenizer(String path){
		List<String> result = Collections.synchronizedList(new ArrayList<String>());
		while (path.contains("/")){
			result.add(path.substring(0,path.indexOf("/")));
			path= path.substring(path.indexOf("/")+1);
		}
		result.add(path);
		return result;
	}
	/**
	 * Constructs a tree with root node C:/ and assumes all 3 chunkserves have that.
	 */
	public Tree (){
		root = new Node();
		root.name="C:";
		root.parent = null;
		root.chunkServersNum.add(1);
		root.chunkServersNum.add(2);
		root.chunkServersNum.add(3);
	}
	/**
	 * Creates a node and sets the chunkserver list for that node equal to the given CSList
	 * Adds the node in the given chunkhandle in master namespace. 
	 * @param paths
	 * @param chunckServersNum
	 * @return
	 */
	public boolean addElement(List<String> paths, List<Integer> chunckServersNum){
		List<String >tempPaths = Collections.synchronizedList(new ArrayList<String>());
		for (int j = 0; j <paths.size()-1; j++)
			tempPaths.add( paths.get(j));
		Node x = root.find(tempPaths, 1);
		if(x==null)
			return false;
		else{
			Node newFile = new Node();
			newFile.name = paths.get(paths.size()-1);
			newFile.parent = x;
			for(Integer CS: chunckServersNum){
				Integer copiedInt = new Integer(CS);
				newFile.chunkServersNum.add(copiedInt);
			}
			x.children.put(newFile.name, newFile);
		}

		return true;
	}
	/**
	 * Removes a given chunkhanlde
	 * @param paths
	 * @return
	 */
	public boolean removeElement(List<String> paths){
		Node x = root.find(paths, 1);
		if (x==null)
			return false;
		else 
			x.parent.children.remove(paths.get(paths.size()-1));
		
		return true;
	}
	/**
	 * Prints all paths starting with the path of a given node.
	 * @param node
	 */
	public void getAllPath(Node node){
		Set<String> allKeys = Collections.synchronizedSet(node.children.keySet());
		Iterator<String> it = allKeys.iterator();
		while (it.hasNext()){
			getAllPath(node.children.get(it.next()));
		}
	}
}
