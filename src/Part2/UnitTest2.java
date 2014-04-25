package Part2;

import java.io.File;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import Client.Client;
import Master.Master;
import Utilities.Node;
import Utilities.Tree;

/*
 * Test2: Create N files in a directory and its subdirectories until the leaf subdirectories.  Each file in a directory is named File1, File2, ..., FileN
	Input:  Path, N
	Functionality:  The Path identifies the root directory and its subdirectories that should have X files.  It might be "1\2" in the above example.
	N is the number of files to create in path and each of its subdirectories
	Note:  When an adversary invokes Test2 twice (or more) in a row, the application should return the meaningful error messages produced by TFS.	
	Example:  Test2 1\2 3
	Assuming the directory structure from the Test1 example above, this Test would create 5 files in each directory 1\2, 1\2\4 and 1\2\5.  The files in each directory would be named File1, File2, and File3.
 */
public class UnitTest2 {

	public static void unitTest3Func(String startingPath, int numFiles, Client myClient) throws RemoteException{
		createNFiles(startingPath, numFiles, myClient);
		Node node = Master.directory.root.find(Tree.pathTokenizer(startingPath), 1);
		Set<String> allKeys = node.children.keySet();
		Iterator<String> it = allKeys.iterator();
		while (it.hasNext()){
			createNFiles(node.children.get(it.next()).getPath(), numFiles, myClient);
		}
	}
	public static void createNFiles(String path, int n, Client myClient) throws RemoteException{
		Random randomGenerator = new Random();
		for (int i = 0; i < n ; i ++){
			int numReplicas = randomGenerator.nextInt(3);
			myClient.createFile(path, "File"+n, numReplicas);
		}
	}
}
