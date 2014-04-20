package Master;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
//import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Semaphore;

import Utilities.Node;
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
	
	//functions called by the client
	public void createFileA(String path, String fileName, int numReplicas, int clientID) throws RemoteException
	{
		
	}
	
	public void deleteFileMasterA(String chunkhandle, int clientID) throws RemoteException
	{
		
	}
	
	public void createDirectoryA(String path, int clientID) throws RemoteException
	{
		

	}
	
	public void deleteDirectoryA(String path, int clientID) throws RemoteException
	{
		
	}
	
	public void appendA(String chunkhandle, int clientID) throws RemoteException
	{

	}
	
	public void atomicAppendA(String chunkhandle, int clientID) throws RemoteException
	{
		ArrayList<String> chunkhandles = new ArrayList<String>();
		chunkhandles.add(chunkhandle);
		Node file = directory.root.find(chunkhandles, 1);
		if(file == null)
		{
			//message client with an error
		}
		else if (file.getPrimaryLeaseTime() < (System.currentTimeMillis() - MINUTE))
		{
			//return the existing primary chunkserver to the client
		}
		else
		{
			Random randInt = new Random();
			int randomCS = randInt.nextInt() % file.chunkServersNum.size();
			file.issuePrimaryLease(file.chunkServersNum.get(randomCS), System.currentTimeMillis());
			//message the client with the chunkservers
		}	
	}

	public void heartbeatA(int CSID) throws RemoteException
	{
		//this function is called when the chunkserver comes back online and an update is required
		
	}

	public static void main(String[] args) {
		Master master = new Master();
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

