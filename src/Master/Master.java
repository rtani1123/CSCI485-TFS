package Master;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
//import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Semaphore;

import Utilities.Tree;

public class Master {

	final static String NOT_FOUND ="Sorry, but the file you had requesting was not found";
	final static long MINUTE = 60000;
	HashMap<String,Metadata> files;
	Tree directory;
	Semaphore stateChange;
	private MasterThread masterThread;
	List<String> tasks;

	public Master() {
		directory = new Tree();
		files = new HashMap<String,Metadata>();

		//System.out.println("1");
		//setupMasterClientServer();
		//System.out.println("2");
		stateChange = new Semaphore(1, true); // binary semaphore
		tasks = Collections.synchronizedList(new ArrayList<String>());
	}

	/*
	 * Threading code
	 * */
	protected void stateChanged() {
		stateChange.release();
	}

	public synchronized void startThread() {
		if (masterThread == null) {
			masterThread = new MasterThread();
			masterThread.start(); // initiates run method in masterThread
		} else {
			masterThread.interrupt();
		}
	}

	public void stopThread() {
		if (masterThread != null) {
			masterThread.stopAgent();
			masterThread = null;
		}
	}

	protected boolean pickAndExecuteAnAction(){
		// scheduler checks, return true if something to do
		return false;
	}

	public boolean createFile(String path, String fileName, int numReplicas) {
		// check for name collision and valid path
		if(files.containsKey(path+"/"+fileName) /*|| !bpt.isValidPath()*/){
			return false;
		}

		// generate unique chunkhandle
		String newChunkhandle = path+"/"+fileName;

		// randomly choose chunkservers to store the file
		ArrayList<Integer> replicaIDs = new ArrayList<Integer>();
		//int[] replicaIDs = new int[numReplicas];
		for(int i = 0; i < numReplicas; i++){
			// add chunkserverID to array randomly
			//int x = new Random(0, numReplicas).nextInt();
			// replicaIDs[i] = ;
		}

		// add file to B tree
		if(directory.addElement(directory.pathTokenizer(path),replicaIDs)){
			// successful add to tree
		}
		else{
			return false;
		}

		// generate a file metadata object and store it in the metadata hashtable
		files.put(newChunkhandle, new Metadata(newChunkhandle));

		// message chunkservers and tell them to create the file (handshake only)
		//for(int i = 0; i < numReplicas; i++) {
		//            int x = new Random(0, numReplicas).nextInt();
		//ChunkserverSocket[x].writeObject(new String(path+”/”+fileName));
		//}

		// return true if successful
		return true;
	}
	public boolean getXLock(String filePath){
		return false;

	}
	public boolean deleteFileMaster(String chunkhandle, String deleteMsg) {
		String parsedDeleteMsg = deleteMsg.replace("$", "");
		ArrayList<String> tokenedMsg = directory.pathTokenizer(parsedDeleteMsg);
		ArrayList<Integer> replicaNum =(directory.root.find(tokenedMsg, 0)).chunkServersNum;
		if (replicaNum==null){
			// send not found message to the client
		}
		else {
			// getXLock(parsedDeleteMsg);
			makeLogRecord(deleteMsg, false, true);
			directory.removeElement(tokenedMsg);
			StringBuffer message = new StringBuffer();
			message.append("$delete$" + deleteMsg);
			//output.write(String.valueOf(message).getBytes());
			//connectToChunkServer();
		}

		// check to see if file exists
		//if doesn’t exist, notify client
		//add delete timestamp to logs
		//search B Tree to find replicas with this file
		//rename files (tag for deletion)  
		//message chunkservers:
		//Chunkservers.write(“delete absoluteFileName”);
		//delete entry from metadata.
		//(if chunkserver down, at next heartbeat, the existence of file will conflict with delete entry in the log.)
		return false;
	}

	boolean createDirectory(String path){
		// check for name collision and valid path
		// add directory to B tree
		// return true if successful
		return false;
	}
	boolean deleteDirectory(String path){
		// recursively check the B tree and get all the files
		// call delete file for each of the files
		return false;
	}
	boolean createSubdirectory(String path){
		// check for name collision and valid path
		// add directory to B tree
		// return true if successful
		return false;
	}
	boolean deleteSubdirectory(String path){        // recursive function
		// check if this has subdirectories within it, if so call self for those directories
		// recursively check the B tree and get all the files
		// call delete file for each of the files
		return false;
	}
	void getMetadata(String chunkhandle, int port){
		Metadata md = files.get(chunkhandle);
		//output.write(new String("meta"));
		StringBuffer message = new StringBuffer("$meta$");
		Map<Integer, Long> replicas = md.getReplicas();

		message.append(md.getNumReplicas());
		message.append("$");

		//this doesn't check for currentness of the timestamp
		for(Map.Entry<Integer,Long> entry: replicas.entrySet()){
			message.append(entry.getKey());
			message.append("$");
		}
	}
	// **this will be a critical section of code for race conditions
	boolean getPrimaryLease(long chunkhandle, int chunkserverID, int port){
		// check to see if a primary lease has been issued

		StringBuffer message = new StringBuffer();
		// check if lease has expired
		if(files.get(chunkhandle).getPrimaryLeaseIssueTime() < System.currentTimeMillis() - MINUTE){
			//give lease
			files.get(chunkhandle).setPrimaryChunkserverLeaseID(chunkserverID);
			message.append("$primary$");
			message.append(chunkhandle);
			message.append("$");
			files.get(chunkhandle).setPrimaryLeaseIssueTime(System.currentTimeMillis());
			message.append(files.get(chunkhandle).getPrimaryLeaseIssueTime());
			message.append("$");
		}
		else{
			// message back
			message.append("$secondary$");
			message.append(files.get(chunkhandle).getPrimaryChunkserverLeaseID());
			message.append("$");
		}

		return false;
	}
	void makeLogRecord(String fileOrDirectoryName, boolean type, boolean stage){
		//generate a string for the appropriate log record and push it onto the log list
	}
	public static void main(String[] args) {
		Master master = new Master();
	}
	protected class HandleHeartbeat implements Runnable {
		Socket mySocket;
		HandleHeartbeat(Socket s) {
			mySocket = s;
		}
		public void run() {

		}
		void parseHeartbeat(Heartbeat hb) {

		}
		protected class Heartbeat {
			Heartbeat() {

			}
		}
	}

	// One metadata instance per file
	protected class Metadata {
		/*
		 *
		 * full path
		 * location of replicas on chunkservers
		 * int IDs of chunkservers with primary lease
		 * most recent write timestamp on those replicas
		 * */

		int primaryChunkserverLeaseID;
		long primaryLeaseIssueTime;
		String fullPath;
		Map<Integer,Long> replicas;

		protected Metadata(String fullPath){
			this.fullPath = fullPath;
			replicas = new HashMap<Integer,Long>();
		}

		// Getters
		public String getFullPath() {
			return fullPath;
		}
		public int getPrimaryChunkserverLeaseID() {
			return primaryChunkserverLeaseID;
		}
		public long getPrimaryLeaseIssueTime() {
			return primaryLeaseIssueTime;
		}

		// Setters
		public void setPrimaryChunkserverLeaseID(int primaryChunkserverLeaseID) {
			this.primaryChunkserverLeaseID = primaryChunkserverLeaseID;
		}
		public void setPrimaryLeaseIssueTime(long l) {
			this.primaryLeaseIssueTime = l;
		}
		public void setFullPath(String fullPath) {
			this.fullPath = fullPath;
		}

		public void addReplica(int ID){
			replicas.put(ID,System.currentTimeMillis());
		}

		public void removeReplica(int ID){
			replicas.remove(ID);
		}

		public void updateWriteTimestamp(int ID){
			replicas.put(ID,System.currentTimeMillis());
		}

		public int getNumReplicas(){
			return replicas.size();
		}

		public Map<Integer, Long> getReplicas() {
			return replicas;
		}
	}




	private class MasterThread extends Thread {
		private volatile boolean goOn = false;

		private MasterThread() {

		}

		public void run() {
			goOn = true;

			while (goOn) {
				try {
					stateChange.acquire();
					while (pickAndExecuteAnAction()) ;
				} catch (InterruptedException e) {
				} catch (Exception e) {
					System.out.println("Unexpected exception caught in Agent thread:" + e);
				}
			}
		}

		private void stopAgent() {
			goOn = false;
			this.interrupt();
		}
	}
}