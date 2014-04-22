package Utilities;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

/**
 * The OperationsLog class provides functionality for the master to use a 
 * log for crash recovery.
 * 
 * Format of the logs is as follows:
 * startTimestamp is the transactionID
 * type = 1 for create and 0 for delete 
 * stage = 0 for received and 1 for commit
 * String: ($startTimestamp$fileOrDirectory$type$stage$)
 */
public class OperationsLog implements Serializable{

	ArrayList<StringBuffer> log;
	FileOutputStream fout;
	ObjectOutputStream cos;

	public OperationsLog() {

	}
	
	/**
	 * makeLogRecord formats input and adds it to the log record ArrayList.
	 * It does not write to disk.
	 * @param startTime
	 * @param type
	 * @param stage
	 * @param chunkhandle
	 */
	public void makeLogRecord(long startTime, int type, int stage, String chunkhandle){
		StringBuffer newRecord = new StringBuffer("$");
		newRecord.append(startTime);
		newRecord.append("$");
		newRecord.append(chunkhandle);
		newRecord.append("$");
		newRecord.append(type);
		newRecord.append("$");
		newRecord.append(stage);
		newRecord.append("$");
		log.add(newRecord);
	}
	
	public void recover(){
		
	}

}
