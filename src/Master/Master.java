package Master;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

import Utilities.BPTree;

public class Master {

	ServerSocket ss; //CLARIFICATION - this SocketServer is only for chunkservers.
	int chunkserverMasterPort = 46344;	//chunkserver port
	ArrayList<Socket> chunkServers;
	
	ObjectOutputStream output;
	ObjectInputStream input;
	
	ClientThreadHandler cth;
	
	BPTree bpt;
	HashMap<String,String> filePaths;
	
	public Master() {
		chunkServers = new ArrayList<Socket>(); //initially empty list of at-some-point-connected chunkservers.
		bpt = new BPTree();
		
		filePaths = new HashMap<String,String>();
		setupServer();
		System.out.println("here");
	}
	
	public void setupServer() {
		try {
			ss = new ServerSocket(chunkserverMasterPort);	//establish ServerSocket
		} catch (Exception e) {
			System.out.println("Port unavailable");
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println("Server Created");
		cth = new ClientThreadHandler();	//new Thread to handle new or rebooting chunkservers
		new Thread(cth).start();
		
	}
	public boolean createFile(String path, String fileName, int numReplicas) {
		// check for name collision and valid path
		
		// generate unique chunkhandle
		    // add file to B tree
		    // generate a file metadata object and store it in the metadata hashtable
		    // message chunkservers and tell them to create the file (handshake only)
		//for(int i = 0; i < numReplicas; i++) {
		//            int x = new Random(0, numReplicas).nextInt();
		//ChunkserverSocket[x].writeObject(new String(path+”/”+fileName));
		//}
		    // set current write timestamp
		    // return true if successful
		return false;
	}
	
	public boolean deleteFile(long chunkhandle) {
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
	void getMetadata(long chunkhandle, int clientID){
	    //send a message to the client at clientID with the appropriate metadata
	}
	boolean getPrimaryLease(long chunkhandle, int chunkserverID){
	    // check to see if a primary lease has been issued 
		return false;
	}
	void makeLogRecord(String fileOrDirectoryName, boolean type, boolean stage){
		//generate a string for the appropriate log record and push it onto the log list
	}
	public static void main(String[] args) {
		Master master = new Master();
	}
	
	class ClientThreadHandler implements Runnable {
		
		public void run() {
			while(true) {
				try {
					Socket s = ss.accept();  //waits for client protocol to connect
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
	
	
}
