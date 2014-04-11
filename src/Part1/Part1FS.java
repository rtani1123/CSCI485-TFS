package Part1;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import Utilities.Tree;

public class Part1FS {

	final static String NOT_FOUND ="Sorry, but the file you had requesting was not found";
	final static long MINUTE = 60000;
	public Tree directory;
	//	OperationsLog log = new OperationsLog();

	public Part1FS() {
		directory = new Tree();
	}
	public Part1FS(Tree inTree){
		if(inTree != null)
		{
			directory = inTree;
			System.out.println("Reading Existing TFS Tree.");
		}
		else
		{
			System.out.println("Creating a new Tree Structure.");
			directory = new Tree();
		}
	}

	public boolean createFile(String path, String fileName, int numReplicas) {
		// check for name collision and valid path
		if(directory.root.find(directory.pathTokenizer(path+"/"+fileName),1) != null){
			System.err.println("File already exists " + path+"/"+fileName);
			return false;
		}

		// add file to tree
		if(directory.addElement(directory.pathTokenizer(path+"/"+fileName),new ArrayList<Integer>())){
			// successful add to tree
		}
		else{
			System.out.println("Element addition to file system failed. Invalid path.");
			return false;
		}

		File f = new File(path+"/"+fileName);
		try {
			if(!f.createNewFile()){
				System.err.println("File creation unsuccessful " + path+"/"+fileName);
			}
		}catch(Exception e){
			e.printStackTrace();
		}

		return true;
	}
	public boolean getXLock(String filePath){
		return false;

	}
	public void deleteFileMaster(String chunkhandle) {
		//		log.makeLogRecord(System.currentTimeMillis(),0, 1, chunkhandle);
		if(directory.root.find(directory.pathTokenizer(chunkhandle),1) != null){
			System.err.println("Nonexistent path " + chunkhandle);
			return;
		}
		if (directory.removeElement(directory.pathTokenizer(chunkhandle)))
		{
			deleteFileChunk(chunkhandle);
		}
		else
		{
			System.out.println("Delete unsuccessful. Item not found.");
		}
	}

	public static boolean deleteFileChunk(String path) {
		String parsedPath = path;
		try {
			File dFile = new File(parsedPath);
			System.out.println("Now Deleting " + parsedPath);
			String[] files =null;
			if(dFile.isDirectory())
				files= dFile.list();
			if (dFile.isFile() || (files.length==0))
				dFile.delete();
			else if (dFile.isDirectory()){
				for (int i = 0; i < files.length; i++){
					deleteFileChunk(parsedPath +"/"+files[i]);
				}
				dFile.delete();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	public boolean createDirectory(String path){
		// check for name collision and valid path
		if(directory.root.find(directory.pathTokenizer(path),1) != null){
			System.err.println("Directory already exists " + path);
			return false;
		}

		// add file to tree
		if(directory.addElement(directory.pathTokenizer(path),new ArrayList<Integer>())){
			// successful add to tree
		}
		else{
			System.out.println("Element addition to file system failed. Invalid path.");
			return false;
		}
		File f = new File(path);
		try {
			if(!f.mkdir())
				System.err.println("Directory creation unsuccessful " + path);
		}catch(Exception e){
			e.printStackTrace();
		}
		return false;
	}
	public void deleteDirectory(String path){
		// getXLock(parsedDeleteMsg);
		//	log.makeLogRecord(System.currentTimeMillis(),0, 1, path);
		if(directory.removeElement(directory.pathTokenizer(path)))
		{
			deleteFileChunk(path);
		}
		else
		{
			System.out.println("Delete unsuccessful. Item not found.");
		}
	}

	//**currently not checking for stale writes using timestamps
	public void append(String chunkhandle, int offset, int length, byte[] data){
		File f = new File(chunkhandle);	
		if(directory.root.find(directory.pathTokenizer(chunkhandle), 1) == null)
		{
			System.out.println("Error. Invalid Path " + chunkhandle);
			return;
		}
		try {
			RandomAccessFile raf = new RandomAccessFile(f,"rws");
			raf.seek(offset);
			raf.write(data);
			raf.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	//**currently not checking for stale writes using timestamps
	public void atomicAppend(String chunkhandle, int length, byte[] data){
		File f = new File(chunkhandle);	
		if(directory.root.find(directory.pathTokenizer(chunkhandle), 1) == null)
		{
			System.out.println("Error. Invalid Path " + chunkhandle);
			return;
		}
		try {
			RandomAccessFile raf = new RandomAccessFile(f,"rws");
			raf.seek(raf.length());
			raf.write(data);
			raf.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void atomicAppendWithSize(String chunkhandle, int length, byte[] data){
		File f = new File(chunkhandle);	// might have to parse chunkhandle into path
		if(directory.root.find(directory.pathTokenizer(chunkhandle), 1) == null)
		{
			System.out.println("Error. Invalid Path " + chunkhandle);
			return;
		}
		try {
			RandomAccessFile raf = new RandomAccessFile(f,"rws");
			raf.seek(raf.length());
			ByteBuffer bb = ByteBuffer.allocate(4);
			bb.putInt(length);
			byte[] result = bb.array();
			raf.write(result);
			raf.write(data);
			raf.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public byte[] read(String chunkhandle, int offset, int length){
		File f = new File(chunkhandle);	// might have to parse chunkhandle into path
		if(directory.root.find(directory.pathTokenizer(chunkhandle), 1) == null)
		{
			System.out.println("Error. Invalid Path " + chunkhandle);
			return new byte[0];
		}
		byte[] b = new byte[length];
		try {
			RandomAccessFile raf = new RandomAccessFile(f,"r");
			raf.seek(offset);
			raf.readFully(b);
			raf.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error in creating RandomAccessFule in read. Returning byte array of 0 size.");
			return new byte[0];
		}
		return b;
	}

	public byte[] readCompletely(String chunkhandle){
		File f = new File(chunkhandle);	
		if(directory.root.find(directory.pathTokenizer(chunkhandle), 1) == null)
		{
			System.out.println("Error. Invalid Path " + chunkhandle);
			return new byte[0];
		}
		byte[] b = new byte[(int)f.length()];
		try {
			RandomAccessFile raf = new RandomAccessFile(f,"r");
			raf.readFully(b);
			raf.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("File read exception");
			return new byte[0];
		}
		return b;
	}

	public long getFileSize(String chunkhandle){
		long size = 0;
		File f = new File(chunkhandle);
		size = f.length();
		return size;
	}

}

