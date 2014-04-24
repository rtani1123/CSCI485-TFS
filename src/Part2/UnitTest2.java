package Part2;

import java.io.File;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Random;

import Client.Client;
import Master.Master;
import Utilities.Storage;

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
	
		ArrayList<String> directories = new ArrayList<String>();
		ArrayList<String> contents = new ArrayList<String>();
		directories.add(startingPath);
		File sp = new File(startingPath);
		if(!sp.isDirectory())
		{
			System.err.println("Error. Not a valid directory.");
			return;
		}
		for (int i = 0; i < sp.list().length; i++)
		{
			contents.add(startingPath + "/" + sp.list()[i]);
		}
		while(contents.size() != 0)
		{
			File f = new File(contents.get(0));
			if (f.isDirectory())
			{
				for (int i = 0; i < f.list().length; i++)
				{
					contents.add(contents.get(0) + "/" + f.list()[i]);
				}
				directories.add(contents.get(0));
			}
			contents.remove(0);
		}
		 Random randomGenerator = new Random();
		//iterate through the directories and create the files
		for (String s : directories)
		{
			for(int i = 0; i < numFiles; i++)
			{
				int numReplicas = randomGenerator.nextInt(3);
				myClient.createFile(s, "File" + (i+1) + ".txt", numReplicas+1);
			}
		}
		System.out.println("Existing tree structure: ");
		Master.directory.getAllPath(Master.directory.root);
		
	}
}
