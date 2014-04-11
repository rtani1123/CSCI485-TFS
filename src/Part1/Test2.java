package Part1;

import java.io.File;
import java.util.ArrayList;
import Utilities.TreeStorage;

/*Test2: Create N files in a directory and its subdirectories until the leaf subdirectories.  
 * Each file in a directory is named File1, File2, ..., FileN

Input:  Path, N
Functionality:  The Path identifies the root directory and its subdirectories that should have X files.  
It might be "1\2" in the above example.
N is the number of files to create in path and each of its subdirectories

Note:  When an adversary invokes Test2 twice (or more) in a row, the application should return the 
meaningful error messages produced by TFS.

Example:  Test2 1\2 3
Assuming the directory structure from the Test1 example above, this Test would create 5 files in each 
directory 1\2, 1\2\4 and 1\2\5.  The files in each directory would be named File1, File2, and File3.
 * */

public class Test2 {
	public static void main(String args[]){
		if (args.length != 2)
		{
			System.err.println("Error. Invalid number of arguments for Test2.");
			return;
		}
		Part1FS tfs = new Part1FS(TreeStorage.getTree());
		String startingPath = args[0];
		int numFiles = Integer.parseInt(args[1]);
		ArrayList<String> directories = new ArrayList<String>();
		ArrayList<String> contents = new ArrayList<String>();
		directories.add(startingPath);
		File sp = new File(startingPath);
		if(!sp.isDirectory())
		{
			System.err.println("Error. Not a valid directory.");
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
		//iterate through the directories and create the files
		for (String s : directories)
		{
			for(int i = 0; i < numFiles; i++)
			{
				tfs.createFile(s, "File" + (i+1) + ".txt", 1);
			}
		}
		System.out.println("Existing tree structure: ");
		tfs.directory.getAllPath(tfs.directory.root);
		TreeStorage.storeTree(tfs.directory);
	}
}
