package Part2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.rmi.RemoteException;

import Client.Client;
import Part1.Part1FS;
import Utilities.Storage;

/*
	 *  Read the content of a TFS file and store it on the specified file on the local machine.
	Input:  TFS file, local file path
	Functionality:  If the TFS file does not exist then return an error message.  
	Otherwise, open the TFS file, read the content of the file, write the content of the file to the local filesystem file.
	Example:  Test5 1\File1.png C:\MyDocument\Pic.png
	If either 1\File1.png does not exist or C:\MyDocument\Pic.png exists then return the appropriate error message.  
	Otherwise, open the TFS file 1\File1.png and read its content into memory.  
	Create and open C:\MyDocument\Pic.png, write the retrieved content into it, and close this file.
	Similar to Test5 from Part 1 with one difference:  It must be able to retrieve a file if at least one of its replicas is available.
 */
public class UnitTest5 {
	public static void unitTest5Func(String src, String dest, Client myClient) throws RemoteException{
		myClient.readCompletely(src, dest);
	}

}
