package Part2;

import java.rmi.RemoteException;
import Client.Client;

/**
 * Test3:  Delete a hierarchical directory structure including the files in those directories.
	Input:  Path 
	Functionality:  The input path identifies the directory whose content along with itself must be deleted.
	Note:  When an adversary invokes Test3 twice (or more) in a row, the application should return the meaningful error messages produced by TFS.	
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

 */
public class UnitTest3 {
	
	
	public static void unitTest3Func(String path, Client myClient) throws RemoteException{
		myClient.deleteFileMaster(path);
	}
}
