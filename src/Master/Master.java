package Master;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
//import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import Utilities.BPTree;

public class Master {

	ServerSocket getClientsSS; //CLARIFICATION - this SocketServer is only for chunkservers.
	int masterClientPort = 56946; //DIFFERENT FROM MASTER/CHUNKSERVER PORT
	int masterChunkserverPortStart = 55501;
	ArrayList<Socket> chunkServers;
	BPTree<String, String> myBPTree = new BPTree<String, String>();
	ObjectOutputStream output;
	ObjectInputStream input;
	
	ChunkserverThreadHandler cth;
	final static String NOT_FOUND ="Sorry, but the file you had requesting was not found";
	final static long MINUTE = 60000;
	BPTree bpt;
	HashMap<String,Metadata> files;
	
	public Master() {
		chunkServers = new ArrayList<Socket>(); //initially empty list of at-some-point-connected chunkservers.
		
		bpt = new BPTree();
		
		files = new HashMap<String,Metadata>();
		setupMasterChunkserverServer();
		System.out.println("here");
	}
	
	public void setupMasterChunkserverServer() {
		
		try {
			getClientsSS = new ServerSocket(49584);	//establish ServerSocket
		} catch (Exception e) {
			System.out.println("Port unavailable");
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println("Server Created");
		cth = new ChunkserverThreadHandler();	//new Thread to handle new or rebooting chunkservers
		new Thread(cth).start();
		
	}
	public void connectToClient() {
		
	}
	public boolean createFile(String path, String fileName, int numReplicas) {
		// check for name collision and valid path
		
		// generate unique chunkhandle
		    // add file to B tree
		    // generate a file metadata object and store it in the metadata hashtable
		    // message chunkservers and tell them to create the file (handshake only)
		//for(int i = 0; i < numReplicas; i++) {
		//            int x = new Random(0, numReplicas).nextInt();
		//ChunkserverSocket[x].writeObject(new String(path+�/�+fileName));
		//}
		    // set current write timestamp
		    // return true if successful
		return false;
	}
	public boolean connectToClient(Object msg){
		return false;
		
	}
	public boolean connectToChunkServer(Object msg, String replicaNum){
		return false;
		
	}
	public boolean getXLock(String filePath){
		return false;
		
	}
	public boolean deleteFileMaster(long chunkhandle, String deleteMsg) {
		String parsedDeleteMsg = deleteMsg.replace("$", "");
		String replicaNum = myBPTree.get(parsedDeleteMsg);
		if (replicaNum==null){
			connectToClient(NOT_FOUND);
		}
		else {
			getXLock(parsedDeleteMsg);
			makeLogRecord(deleteMsg, false, false); // what are the inputs to this func?
			boolean b = connectToChunkServer(deleteMsg, replicaNum);
			if (b){//means that it has been deleted successfully
				//add delete function to BPTree
				// delete this from BPTree
			}
			
		}
			
		// check to see if file exists
	    //if doesn�t exist, notify client
	    //add delete timestamp to logs
	    //search B Tree to find replicas with this file
		//rename files (tag for deletion)    
		//message chunkservers:
	    //Chunkservers.write(�delete absoluteFileName�);
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
		try{
			// **look up output stream for this port
			output.write(String.valueOf(message).getBytes());
		}catch(Exception e){
			
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
		
		try{
			// **look up output stream for this port
			output.write(String.valueOf(message).getBytes());
		}catch(Exception e){
			
		}
		return false;
	}
	void makeLogRecord(String fileOrDirectoryName, boolean type, boolean stage){
		//generate a string for the appropriate log record and push it onto the log list
	}
	public static void main(String[] args) {
		Master master = new Master();
	}
	
	class ChunkserverThreadHandler implements Runnable {
		
		public void run() {
			while(true) {
				try {
					Socket s = getClientsSS.accept();  //waits for client protocol to connect
					chunkServers.add(s);  //list of master's chunkservers
					createClientThread(s);	//connect input/output streams
				} catch(Exception e) {
					System.out.println("Socket wasn't able to add");
					e.printStackTrace();
				}
			}
		} //end run

		private void createClientThread(Socket s) {
			ObjectOutputStream out = null;
			ObjectInputStream in = null;
			try {
				out = new ObjectOutputStream(s.getOutputStream());
				in = new ObjectInputStream(s.getInputStream());
			} catch (Exception e) {
				System.out.println("Streams unable to connect to socket");
				e.printStackTrace();
				System.exit(0);
			}
		} //end createClientThread
	} //end class ClientThreadHandler

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
		
		
		protected Metadata(){
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
	
}
