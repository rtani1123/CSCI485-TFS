package Part1;

import Utilities.Storage;

/*Test3:  Delete a hierarchical directory structure including the files in those directories.
Input:  Path
Functionality:  The input path identifies the directory whose content along with itself must be deleted.

Note:  When an adversary invokes Test3 twice (or more) in a row, the application should return the 
meaningful error messages produced by TFS.

Example:  Test3 1\2
Assuming the directory sturcture from Test2 above, this test would delete 3 directories and 9 files.
The deleted directories are 1\2, 1\2\4 and 1\2\5.  The fires deleted are:
1\2\File1
1\2\File2
1\2\File3
1\2\4\File1
1\2\4\File2
1\2\4\File3
1\2\5\File1
1\2\5\File2
1\2\5\File3

 * */

public class Test3 {
	public static void main(String args[]){
		if (args.length != 1)
		{
			System.err.println("Error. Invalid number of arguments for Test3.");
			return;
		}
		Part1FS tfs = new Part1FS(Storage.getTree());
		String startingPath = args[0];
		//maybe throw in some fail safe here?
		tfs.deleteDirectory(startingPath);
		System.out.println("Existing tree structure: ");
		tfs.directory.getAllPath(tfs.directory.root);
		Storage.storeTree(tfs.directory);
	}
}
