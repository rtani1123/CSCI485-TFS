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
		//return an error if destination file already exists or if starting file does not exist
		Part1FS tfs = new Part1FS(TreeStorage.getTree());
		String startingFullPath = args[0];
		String destinationFullPath = args[1];
		File localDest = new File(destinationFullPath);
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
