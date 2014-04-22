package Part1;

import java.io.File;
import java.nio.ByteBuffer;
import Utilities.Storage;

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
		if (args.length != 1)
		{
			System.err.println("Error. Invalid number of arguments for Test7.");
			return;
		}
		Part1FS tfs = new Part1FS(Storage.getTree());
		String fullPath = args[0];
		File f = new File(fullPath);
		if (!f.exists())
		{
			System.out.println("Specified file does not exist on TFS server");
			return;			
		}			
		else if(f.length() <4)
		{
			System.out.println("File too small to contain a size");
			return;
		}
		byte[] szb = tfs.read(fullPath, 0, 4);
		ByteBuffer wrapped = ByteBuffer.wrap(szb);
		int sz = wrapped.getInt();
		int offset = 4;
		int count = 1;
		while(offset < f.length())
		{
			System.out.println("Size of file " + count + " is " + sz + " bytes");
			//byte[] payload = tfs.read(fullPath, offset, sz);
			offset += sz;
			//read the next file size in
			if(offset < f.length())
			{
				szb = tfs.read(fullPath, offset, 4);
				ByteBuffer wrapped2 = ByteBuffer.wrap(szb);
				sz = wrapped2.getInt();
				offset += 4;
				count++;
			}
		}
		System.out.println(fullPath + " contains " + count + " separate files");
		Storage.storeTree(tfs.directory);
	}
}
