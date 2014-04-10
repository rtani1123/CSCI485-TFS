package Part1;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import Master.OperationsLog;
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
		directory = inTree;
	}

	public boolean createFile(String path, String fileName, int numReplicas) {
		// check for name collision and valid path
		if(directory.root.find(directory.pathTokenizer(path+"/"+fileName),1) != null){
			System.err.println("File already exists" + path+"/"+fileName);
			return false;
		}

		// add file to tree
		if(directory.addElement(directory.pathTokenizer(path+"/"+fileName),new ArrayList<Integer>())){
			// successful add to tree
		}
		else{
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
		directory.removeElement(directory.pathTokenizer(chunkhandle));
		deleteFileChunk(chunkhandle);
	}

	public static boolean deleteFileChunk(String path) {
		String parsedPath = path;
		try {
			File dFile = new File(parsedPath);
			System.out.println(parsedPath);
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
			return false;
		}

		// add file to tree
		if(directory.addElement(directory.pathTokenizer(path),new ArrayList<Integer>())){
			// successful add to tree
		}
		else{
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
//		log.makeLogRecord(System.currentTimeMillis(),0, 1, path);
		directory.removeElement(directory.pathTokenizer(path));
		deleteFileChunk(path);
	}

	//**currently not checking for stale writes using timestamps
	public void append(String chunkhandle, int offset, int length, byte[] data){
		File f = new File(chunkhandle);	// might have to parse chunkhandle into path
		try {
			RandomAccessFile raf = new RandomAccessFile(f,"rws");
			raf.write(data, offset, length);
			// change write timestamp
			//files.get(chunkhandle).setWriteTime(System.currentTimeMillis());
			raf.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	//**currently not checking for stale writes using timestamps
	public void atomicAppend(String chunkhandle, int length, byte[] data){
		File f = new File(chunkhandle);	// might have to parse chunkhandle into path
		try {
			RandomAccessFile raf = new RandomAccessFile(f,"rws");
			raf.seek(raf.length());
			raf.write(data);
			// change write timestamp
			//files.get(chunkhandle).setWriteTime(System.currentTimeMillis());
			raf.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public byte[] read(String chunkhandle, int offset, int length){
		File f = new File(chunkhandle);	// might have to parse chunkhandle into path
		byte[] b = new byte[length];
		try {
			RandomAccessFile raf = new RandomAccessFile(f,"r");
			raf.seek(raf.length());
			raf.readFully(b,offset,length);
			// change write timestamp
			//files.get(chunkhandle).setReadTime(System.currentTimeMillis());
			raf.close();
		} catch (Exception e) {
			e.printStackTrace();
			// might want to send a message with some error
			return new byte[0];
		}
		return b;
	}
	
	public byte[] readCompletely(String chunkhandle){
		File f = new File(chunkhandle);	// might have to parse chunkhandle into path
		byte[] b = new byte[(int)f.length()];
		try {
			RandomAccessFile raf = new RandomAccessFile(f,"r");
			raf.readFully(b);
			// change write timestamp
			//files.get(chunkhandle).setReadTime(System.currentTimeMillis());
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

