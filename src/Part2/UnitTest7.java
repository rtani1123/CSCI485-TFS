package Part2;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.rmi.RemoteException;

import Client.Client;
import Part1.Part1FS;
import Utilities.Storage;

/*
 * Test 7:  Count the number of logical files stored in a TFS file using Test6 and printout the results.
 Input:  A TFS file generated using Test6
 Functionality:  If the input TFS file does not exist then return an error.  Otherwise, counts the number of logical files stored in a TFS file (generated using Test6) by reading the size and payload pairs in the specified file name.
 Example:  Test7 1/File1.haystack
 Assumption:  Input file, 1/File1.haystack, is generated using Test6.
 Unit7:  Identical to Test7 from Part 1. 
 Note:  Make sure your TFS file system does not become confused if a chunkserver with a replica goes down, Unit6 is invoked on an existing file, the failed chunkserver is brought on line, and we issue the Unit7.  It must reflect the latest count and bring the replica of the failed server up to date.

 */
public class UnitTest7 {
	public static void unitTest7Func(String src, Client myClient)
			throws RemoteException{
		myClient.numFiles(src);
	}
}
