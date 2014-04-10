package Part1;

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
		Part1FS tfs = new Part1FS();
	}
}
