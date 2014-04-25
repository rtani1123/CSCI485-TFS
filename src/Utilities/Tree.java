package Utilities;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class Tree implements Serializable {
	public Node root;
	public static void main(String[] args) {
		List<String> paths = Collections.synchronizedList(new ArrayList<String>());
		Tree myTree= new Tree();
//		ArrayList<Integer> chunkServersNum = new ArrayList<>();
//		chunkServersNum.add(1);
//		paths = myTree.pathTokenizer("C:/Users");
//		myTree.addElement(paths,chunkServersNum );
//		paths = myTree.pathTokenizer("C:/Users/Download");
//		myTree.addElement(paths, chunkServersNum);
//		paths = myTree.pathTokenizer("C:/Program");
//		myTree.addElement(paths, chunkServersNum);
//		System.out.println(x.getPath());
		Storage ts = new Storage();
		myTree = ts.getTree();
//		ts.storeTree(myTree);
//		myTree = ts.getTree();
//		Node x = myTree.root.find(paths, 1);
//		System.out.println(x.getPath());
		myTree.getAllPath(myTree.root);
//		myTree.removeElement(paths);
//		
//		Node y = myTree.root.find(paths, 1);
//		System.out.println(y==null);
	}
	public static List<String> pathTokenizer(String path){
		List<String> result = Collections.synchronizedList(new ArrayList<String>());
		while (path.contains("/")){
			result.add(path.substring(0,path.indexOf("/")));
			path= path.substring(path.indexOf("/")+1);
		}
		result.add(path);
		return result;
	}
	
	public Tree (){
		root = new Node();
		root.name="C:";
		root.parent = null;
		root.chunkServersNum.add(1);
		root.chunkServersNum.add(2);
		root.chunkServersNum.add(3);
	}
	
	public boolean addElement(ArrayList<String> paths, ArrayList<Integer> chunckServersNum){
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
	public boolean removeElement(ArrayList<String> paths){
		Node x = root.find(paths, 1);
		if (x==null)
			return false;
		else 
			x.parent.children.remove(paths.get(paths.size()-1));
		
		return true;
	}
	public void getAllPath(Node node){
		System.out.println(node.getPath());
		Set<String> allKeys = Collections.synchronizedSet(node.children.keySet());
		Iterator<String> it = allKeys.iterator();
		while (it.hasNext()){
			getAllPath(node.children.get(it.next()));
		}
	}
}
