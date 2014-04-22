package Part1;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import Utilities.Storage;

/*Test4:  Store a file on the local machine in a target TFS file specified by its path. 

Input:  local file path, TFS file

Functionality:  If the TFS file exists then reutrn an error message.  
Otherwise, create the TFS file, read the conent of the local file and store it in the TFS File.

Example:  Test4 C:\MyDocuments\Image.png 1\File1.png
If 1\File1.png exists then reutrn error.  Otherwise, create 1/File1.png, 
read the content of C:\MyDocument\Image.png, write the retrieved content into 1\File1.png
 * */

public class Test4 {
	public static void main(String args[]){
		if (args.length != 2)
		{
			System.err.println("Error. Invalid number of arguments for Test4.");
			return;
		}
		Part1FS tfs = new Part1FS(Storage.getTree());
		String startingFullPath = args[0];
		String destinationFullPath = args[1];
		File destinationFile = new File(destinationFullPath);
		File inputFile = new File(startingFullPath);
		if(destinationFile.exists())
		{
			//return an error if destination file already exists
			System.err.println("Error. Destination file already exists on TFS.");
			return;
		}
		if(!inputFile.exists())
		{
			//return an error if destination file already exists
			System.err.println("Error. Input file not found.");
			return;
		}
		int lastSlash = destinationFullPath.lastIndexOf("/", destinationFullPath.length());
		String destinationFileName = destinationFullPath.substring(lastSlash+1, destinationFullPath.length());
		String destinationPath = destinationFullPath.substring(0, lastSlash);
		tfs.createFile(destinationPath, destinationFileName, 1);
		byte[] b = new byte[(int)inputFile.length()];
		//read in the input file
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
		//write to the output file
		tfs.append(destinationFullPath, 0, b.length, b);
		System.out.println("Successful TFS file creation " + destinationFullPath);
		Storage.storeTree(tfs.directory);
	}
}
