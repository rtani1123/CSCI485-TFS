package Utilities;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class TreeStorage {
static public Tree getTree(){
	String filename = "tree.txt";
	Tree myTree = null;
//	List pDetails = null;
	File f = new File (filename);
	if(!f.exists())
	{
		System.out.println("No stored tree file exists.");
		return null;
	}
	FileInputStream fis = null;
	ObjectInputStream oin = null;
	try {
		if(f.length() > 0)
		{
			fis = new FileInputStream(filename);
			oin = new ObjectInputStream(fis);
			myTree = (Tree) oin.readObject();
			oin.close();
		}
	} catch (IOException ex) {
		ex.printStackTrace();
	} catch (ClassNotFoundException ex) {
		ex.printStackTrace();
	}
	return myTree;
}
static public void storeTree(Tree inTree){
	String filename = "tree.txt";
	FileOutputStream fos = null;
	ObjectOutputStream out = null;
	try {
		fos = new FileOutputStream(filename);
		out = new ObjectOutputStream(fos);
		out.writeObject(inTree);
		out.close();
		System.out.println("Namespace Tree Persisted");
	} catch (IOException ex) {
		ex.printStackTrace();
	}
}
}
