package Part1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

import Utilities.TreeStorage;

/*
 * Test1:  Create a hierarchical directory structure.  
 * Its input is the number of directories to create and is a value greater than 1.  
 * This test program creates a directory named "1" and two subdirectories underneath it, 2 and 3.  
 * It repeats the process for these subdirectories recursively creating a subdirectory for 
 * each leaf directory until it has created the total number of specified directories.
  
Input:  an integer denoting the number of directories

Note:  When an adversary invokes Test1 twice (or more) in a row, 
the application should return the meaningful error messages produced by TFS.

Example:  Test1 7
With the input value 7, the resulting directory structure would be
1
1\2
1\3
1\2\4
1\2\5
1\3\6
1\3\7
*/

public class Test1 {

	
	
	public static void main(String args[]){
		Part1FS tfs = new Part1FS();
		int numFolders = Integer.parseInt(args[0]);
		// create root'
		tfs.directory.root.name="C:/1";
		tfs.createDirectory("C:/1");
		for(int i = 2; i <= numFolders; i++){
			int k = i;
			StringBuffer path = new StringBuffer(String.valueOf(k));
			k = (int)Math.floor((double)k/2);
			while (k > 0)
			{
				String addMe = k + "/";
				path.insert(0, addMe);
				k = (int)Math.floor((double)k/2);
			}
			path.insert(0, "C:/");
			tfs.createDirectory(path.toString());
			System.out.println(path);
		}
		tfs.createFile("C:/1/2", "bob.png", 1);
		TreeStorage.storeTree(tfs.directory);
//		File inputFile = new File("C:/Users/bquock/SAC.png");
//		byte[] b = new byte[(int)inputFile.length()];
//		try{
//			FileInputStream
//		}
//		catch(FileNotFoundException fnfe){
//			
//		}
//		catch(IOException ioe){
//			
//		}
	}
}
