package Part1;

import java.io.File;

import Utilities.Node;
import Utilities.TreeStorage;

/*Test 7:  Count the number of logical files stored in a TFS file using Test6 and printout the results.

Input:  A TFS file generated using Test6

Functionality:  If the input TFS file does not exist then return an error.  
Otherwise, counts the number of logical files stored in a TFS file (generated using Test6) by 
reading the size and payload pairs in the specified file name.

Example:  Test7 1/File1.haystack

Assumption:  Input file, 1/File1.haystack, is generated using Test6.
 * */

public class Test7 {
	public static void main(String args[]){
		Part1FS tfs = new Part1FS(TreeStorage.getTree());
		String fullPath = args[0];
//		Node x = tfs.directory.root.find(tfs.directory.pathTokenizer(fullPath), 1);
//		if(x == null)
//		{
//			//the file does not exist
//			System.out.println("Specified TFS File Does Not Exist");
//		}
		//read in the first four bytes 
		File f = new File(fullPath);
		if(f.length() <4)
		{
			System.out.println("File too small to contain a size");
			return;
		}
		byte[] szb = tfs.read(fullPath, 0, 4);
		int sz = 0;
		int offset = 4;
		int count = 1;
		for (int i = 0; i < szb.length; i++)
		{
		   sz += ((long) szb[i] & 0xffL) << (8 * i);
		}
		while(offset < f.length())
		{
			System.out.println(sz);
			//byte[] payload = tfs.read(fullPath, offset, sz);
			offset += sz;
			//read the next file size in
			if(offset < f.length())
			{
				szb = tfs.read(fullPath, offset, 4);
				sz = 0;
				for (int i = 0; i < szb.length; i++)
				{
				   sz += ((long) szb[i] & 0xffL) << (8 * i);
				}
				offset += 4;
				count++;
			}
		}
		System.out.println(fullPath + " contains " + count + " separate files");
		System.out.println("Existing tree structure: ");
		tfs.directory.getAllPath(tfs.directory.root);
		TreeStorage.storeTree(tfs.directory);
	}
}
