package Part2;
/*
	 * Test4:  Store a file on the local machine in a target TFS file specified by its path. 
	Input:  local file path, TFS file
	Functionality:  If the TFS file exists then reutrn an error message.  Otherwise, create the TFS file, read the conent of the local file and store it in the TFS File.
	Example:  Test4 C:\MyDocuments\Image.png 1\File1.png
	If 1\File1.png exists then reutrn error.  Otherwise, create 1/File1.png, read the content of C:\MyDocument\Image.png, write the retrieved content into 1\File1.png
	Test5:  Read the content of a TFS file and store it on the specified file on the local machine.
	Input:  TFS file, local file path
	Functionality:  If the TFS file does not exist then return an error message.  Otherwise, open the TFS file, read the content of the file, write the content of the file to the local filesystem file.
	Example:  Test5 1\File1.png C:\MyDocument\Pic.png
	If either 1\File1.png does not exist or C:\MyDocument\Pic.png exists then return the appropriate error message.  Otherwise, open the TFS file 1\File1.png and read its content into memory.  Create and open C:\MyDocument\Pic.png, write the retrieved content into it, and close this file.
	Unit4:  Identical to Test4 from Part 1 with number of replicas as an additional input.  
	Input:  TFS file, local file path, Number of replicas
	Number of replicas must be a value greater than zero.  Raise an error and do not create the file if the
	specified number of replicas is greater than the number of chunk servers.
 */
public class UnitTest4 {

}
