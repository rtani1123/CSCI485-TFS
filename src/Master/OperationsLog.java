package Master;

import java.io.Serializable;
import java.util.ArrayList;

public class OperationsLog implements Serializable{

	ArrayList<String> log;
	
	OperationsLog() {
		
	}
	
	/*
	 * LOGGING FORMAT
		startTimestamp is the transactionID
		type = 1 for create and 0 for delete 
		stage = 0 for received and 1 for commit
		String: ($startTimestamp$fileOrDirectory$type$stage$)
	 */
}
