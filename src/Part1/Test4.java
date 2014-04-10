package Part1;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import Utilities.TreeStorage;

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
		//return an error if destination file already exists
		Part1FS tfs = new Part1FS(TreeStorage.getTree());
		String startingFullPath = args[0];
		String destinationFullPath = args[1];
		int lastSlash = destinationFullPath.lastIndexOf("/", destinationFullPath.length());
		String destinationFileName = destinationFullPath.substring(lastSlash+1, destinationFullPath.length());
		String destinationPath = destinationFullPath.substring(0, lastSlash);
		tfs.createFile(destinationPath, destinationFileName, 1);
		File inputFile = new File(startingFullPath);
		byte[] b = new byte[(int)inputFile.length()];
		//read in the input file
		try{
			FileInputStream fis = new FileInputStream(inputFile);
			fis.read(b);
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
		TreeStorage.storeTree(tfs.directory);
	}
}
