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
	public static void unitTest7Func(String src, String dest, Client myClient)
			throws RemoteException {
		myClient.read(src, 0, 4, dest);
		File f = new File(dest);
		if (!f.exists()) {
			System.out.println("Specified file does not exist on TFS server");
			return;
		} else if (f.length() < 4) {
			System.out.println("File too small to contain a size");
			return;
		}
		byte[] szb = new byte[(int) f.length()];
		try {
			RandomAccessFile raf = new RandomAccessFile(f, "r");
			raf.readFully(szb);
			raf.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("File read exception");
		}
		ByteBuffer wrapped = ByteBuffer.wrap(szb);
		int sz = wrapped.getInt();
		int offset = 4;
		int count = 1;
		while (offset < f.length()) {
			System.out.println("Size of file " + count + " is " + sz + " bytes");
			offset += sz;
			// read the next file size in
			if (offset < f.length()) {
				f = new File(dest);
				myClient.read(src, offset, 4, dest);
				szb = new byte[(int) f.length()];
				try {
					RandomAccessFile raf = new RandomAccessFile(f, "r");
					raf.readFully(szb);
					raf.close();
				} catch (Exception e) {
					e.printStackTrace();
					System.err.println("File read exception");
				}
				ByteBuffer wrapped2 = ByteBuffer.wrap(szb);
				sz = wrapped2.getInt();
				offset += 4;
				count++;
			}
		}
		System.out.println(src + " contains " + count + " separate files");
	}
}
