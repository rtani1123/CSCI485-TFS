package Part1;

import java.util.ArrayList;

import Utilities.Node;
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
		Part1FS tfs = new Part1FS(TreeStorage.getTree());
		
		
	}
}
