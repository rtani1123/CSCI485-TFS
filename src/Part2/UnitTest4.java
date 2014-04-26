package Part2;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.RemoteException;
import Client.Client;


/**
	 * Test4:  Store a file on the local machine in a target TFS file specified by its path. 
	Input:  local file path, TFS file
	Functionality:  If the TFS file exists then reutrn an error message.  Otherwise, create the TFS file, read the conent of the local file and store it in the TFS File.
	Example:  Test4 C:\MyDocuments\Image.png 1\File1.png
	If 1\File1.png exists then reutrn error.  Otherwise, create 1/File1.png, read the content of C:\MyDocument\Image.png, write the retrieved content into 1\File1.png
	Unit4:  Identical to Test4 from Part 1 with number of replicas as an additional input.  
	Input:  TFS file, local file path, Number of replicas
	Number of replicas must be a value greater than zero.  Raise an error and do not create the file if the
	specified number of replicas is greater than the number of chunk servers.
 */
public class UnitTest4 {
	public static void unitTest4Func(String src, String dest, int numReplicas, Client myClient) throws RemoteException{
		if (numReplicas<1){
			System.out.println("Please enter a number greater than 0 as the number of replicas");
			return;
		}
		//create the file
		int lastSlash = dest.lastIndexOf("/", dest.length());
		String destinationFileName = dest.substring(lastSlash+1, dest.length());
		String destinationPath = dest.substring(0, lastSlash);
		myClient.createFile(destinationPath, destinationFileName, numReplicas);
		//read in the input file
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
		//append to file
		myClient.atomicAppend(dest, b.length, b, false);
	}
}
