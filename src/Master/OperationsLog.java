package Master;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

public class OperationsLog implements Serializable{

	ArrayList<StringBuffer> log;
	FileOutputStream fout;
	ObjectOutputStream cos;

	public OperationsLog() {
//		try {
//			fout = new FileOutputStream("C:\\CS485\\transactionlog.ser");
//			cos = new ObjectOutputStream(fout);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
	}

	/*LOGGING FORMAT
startTimestamp is the transactionID
type = 1 for create and 0 for delete 
stage = 0 for received and 1 for commit
String: ($startTimestamp$fileOrDirectory$type$stage$)
	 * */
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

	/*
	 * LOGGING FORMAT
		startTimestamp is the transactionID
		type = 1 for create and 0 for delete 
		stage = 0 for received and 1 for commit
		String: ($startTimestamp$fileOrDirectory$type$stage$)
	 */
}
