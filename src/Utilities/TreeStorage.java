package Utilities;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class TreeStorage {
public Tree getTree(){
	String filename = "C:/Users/boghrati/tree.txt";
	Tree myTree = null;
//	List pDetails = null;
	FileInputStream fis = null;
	ObjectInputStream oin = null;
	try {
		fis = new FileInputStream(filename);
		oin = new ObjectInputStream(fis);
		myTree = (Tree) oin.readObject();
		oin.close();
	} catch (IOException ex) {
		ex.printStackTrace();
	} catch (ClassNotFoundException ex) {
		ex.printStackTrace();
	}
	return myTree;
}
public void storeTree(Tree inTree){
	String filename = "C:/Users/boghrati/tree.txt";
	FileOutputStream fos = null;
	ObjectOutputStream out = null;
	try {
		fos = new FileOutputStream(filename);
		out = new ObjectOutputStream(fos);
		out.writeObject(inTree);
		out.close();
		System.out.println("Object Persisted");
	} catch (IOException ex) {
		ex.printStackTrace();
	}
}
}
