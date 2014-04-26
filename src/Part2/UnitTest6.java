package Part2;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.RemoteException;
import Client.Client;


/*
 * Test6:  Append the size and content of a file stored on the local machine in a target TFS file specified by its path.
	Input: local file path, TFS file
	Functionality:  If the TFS file does not exists then create the specified file.  Read the content of the local file, compute the number of bytes read, seek to the end of the TFS file, append a 4 byte integer pertaining to the image size, append the content in memory after these four bytes, and close the TFS file.
	Example:  Test6 C:\MyDocuments\Image.png 1\File1.png
	If 1\File1.png exists then create the TFS file 1\File1.png.  Read the content of C:\MyDocument\Image.png into an array of bytes named B and perform the following steps: 1) Let s denote the number of bytes retrieved, i.e., size of B, 2) Let delta=0, 3) Seek to offset delta of 1\File1.png, 4) Try to read 4 bytes, 5)  If EOF then append 4 bytes corresponding to s followed with the array of bytes B, otherwise interpret the four bytes as the integer k and set delta=delta+4+k and goto Step 3.  (The objective of the iteration is to reach the EOF.)
	Assumptions:  Test6 assumes a seek operation is relative to the start of the file.  
	Note:  Make sure the definition of seek is consistent with your design definition of seek.
	 Similar to Test6 from Part 1 with one difference:  If the input file does not exist then create with the number of replicas set to 3.  See Part 1 for details.
 */
public class UnitTest6 {

	public static void unitTest6Func(String src, String dest, Client myClient) throws RemoteException{
		//read file
		File inputFile = new File(src);
		byte[] b = new byte[(int)inputFile.length()];
		try{
			FileInputStream fis = new FileInputStream(inputFile);
			fis.read(b);
			fis.close();
		}
		catch(FileNotFoundException fnfe){
			System.err.println("File not found.");
			fnfe.printStackTrace();
		}
		catch(IOException ioe){
			System.err.println("Error reading the file.");
			ioe.printStackTrace();			
		}
		//create file
		int lastSlash = dest.lastIndexOf("/", dest.length());
		String destinationFileName = dest.substring(lastSlash+1, dest.length());
		String destinationPath = dest.substring(0, lastSlash);
		try{
			myClient.createFile(destinationPath, destinationFileName, 3);
			//append to file with size
			myClient.atomicAppend(dest, b.length, b, true);
		}
		catch(RemoteException re){
			System.out.println("Error in connection between client and application - Unit 6");
		}
	}
}
