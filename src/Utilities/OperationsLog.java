package Utilities;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The OperationsLog class provides functionality for the master to use a 
 * log for crash recovery.
 * 
 * Format of the logs is as follows:
 * startTimestamp is the transactionID
 * type = read, createFile, deleteFile, createDirectory, deleteDirectory, append, or atomicAppend\
 * stage is 0 for start and 1 for commit
 * String: ($startTimestamp$fileOrDirectory$type$stage$)
 */
public class OperationsLog implements Serializable{
	static Integer lock = -1;
	List<StringBuffer> log;
	FileOutputStream fout;
	ObjectOutputStream cos;

	public OperationsLog() {
		log = Collections.synchronizedList(new ArrayList<StringBuffer>());
	}
	
	/**
	 * makeLogRecord formats input and adds it to the log record ArrayList.
	 * It does not write to disk.
	 * @param startTime
	 * @param type
	 * @param chunkhandle
	 */
	synchronized public void makeLogRecord(long startTime, String chunkhandle, String type, int stage){
		StringBuffer newRecord = new StringBuffer("$");
		newRecord.append(startTime);
		newRecord.append("$");
		newRecord.append(chunkhandle);
		newRecord.append("$");
		newRecord.append(type);
		newRecord.append("$");
		newRecord.append(stage);
		newRecord.append("$");
		synchronized(lock) {
			log.add(newRecord);
		}
	}
	
	synchronized public String getReference(int index){
		String reference;
		synchronized(lock){
			reference = log.get(index).toString();
		}
		return reference;
	}
	
	public int getLength(){
		return log.size();
	}
	
	public void recover(){
		
	}

}
