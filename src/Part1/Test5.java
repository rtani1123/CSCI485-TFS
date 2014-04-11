package Part1;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import Utilities.TreeStorage;

/*Test5:  Read the content of a TFS file and store it on the specified file on the local machine.
Input:  TFS file, local file path

Functionality:  If the TFS file does not exist then return an error message.  
Otherwise, open the TFS file, read the content of the file, write the content of the file to the 
local filesystem file.

Example:  Test5 1\File1.png C:\MyDocument\Pic.png
If either 1\File1.png does not exist or C:\MyDocument\Pic.png exists then return the appropriate 
error message.  Otherwise, open the TFS file 1\File1.png and read its content into memory.  
Create and open C:\MyDocument\Pic.png, write the retrieved content into it, and close this file.

 * */

public class Test5 {
	public static void main(String args[]){
		if (args.length != 2)
		{
			System.err.println("Error. Invalid number of arguments for Test5.");
			return;
		}
		Part1FS tfs = new Part1FS(TreeStorage.getTree());
		String startingFullPath = args[0];
		String destinationFullPath = args[1];
		File localDest = new File(destinationFullPath);
		File sourceFile = new File(startingFullPath);
		if(localDest.exists())
		{
			System.out.println("Error. File already exists on local system.");
			return;
		}
		if(!sourceFile.exists())
		{
			System.out.println("Error. Source file not found in TFS.");
			return;
		}
		try {
			localDest.createNewFile();
		} catch (IOException e) {
			System.err.println("Local file creation failure.");
			e.printStackTrace();
		}
		byte[] b = new byte[(int)tfs.getFileSize(startingFullPath)];
		//read in the input file
		b = tfs.readCompletely(startingFullPath);
		//write to the output file
		try
		{
			FileOutputStream fos = new FileOutputStream(localDest);
			fos.write(b);
			fos.flush();
			fos.close();
			System.out.println("Successful local file creation " + destinationFullPath);
		}
		catch(FileNotFoundException fnfe){
			System.err.println("File not found.");
			fnfe.printStackTrace();
		}
		catch(IOException ioe){
			System.err.println("Error writing to the file.");
			ioe.printStackTrace();			
		}
		TreeStorage.storeTree(tfs.directory);
	}
}
