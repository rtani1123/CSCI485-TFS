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

import Interfaces.ChunkserverInterface;
import Interfaces.ClientInterface;
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
	ClientInterface client;
	HashMap<Integer, ChunkserverInterface> chunkservers;

	public Master() {
		directory = new Tree();
		files = new HashMap<String,Metadata>();
		chunkservers = new HashMap<Integer, ChunkserverInterface>();
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
		Node file = directory.root.find(directory.pathTokenizer(path), 1);	
		if (file != null){
			//if the file exists, we return an error
			client.requestStatus("createFile", path + "/" + fileName, false, -1);
		}
		else{
			//pick the number of replicas
			ArrayList<Integer> CSLocations = new ArrayList<Integer>();
			if (numReplicas == 3){
				//all of the chunkservers get the file
				CSLocations.add(1);
				CSLocations.add(2);
				CSLocations.add(3);
			}
			else{
				//randomly pick the chunkservers to have the file
				int count = 0;
				Random rands = new Random();
				while (count < numReplicas){
					int CS = rands.nextInt() % chunkservers.size();
					if (!CSLocations.contains(CS)){
						count++;
						CSLocations.add(CS);
					}
				}
			}
			if(directory.addElement(directory.pathTokenizer(path+"/"+fileName),CSLocations)){
				System.out.println("Successful add to tree. Requesting file create from CS.");
				for(Integer CS : CSLocations){
					chunkservers.get(CS).createFile(path + "/" + fileName);
				}
				client.requestStatus("createFile", path + "/" + fileName, true, -1);
			}
			else{
				System.out.println("Element addition to file system failed. Invalid path.");
				client.requestStatus("createFile", path + "/" + fileName, false, -1);
			}
		}
	
	}
	
	public void deleteFileMasterA(String chunkhandle, int clientID) throws RemoteException
	{
		Node file = directory.root.find(directory.pathTokenizer(chunkhandle),1);
		if(file == null){
			System.err.println("Nonexistent path " + chunkhandle);
			client.requestStatus("deleteFile", chunkhandle, false, -1);
		}
		else if(directory.removeElement(directory.pathTokenizer(chunkhandle)))
		{
			for(Integer CS : file.chunkServersNum){
				chunkservers.get(CS).deleteDirectory(chunkhandle);
			}
			client.requestStatus("deleteFile", chunkhandle, true, -1);
		}
		else
		{
			System.out.println("Delete unsuccessful. Item not found.");
			client.requestStatus("deleteFile", chunkhandle, false, -1);
		}
	}
	
	public void createDirectoryA(String path, int clientID) throws RemoteException{
		Node newDir = directory.root.find(directory.pathTokenizer(path), 1);	
		if (newDir != null){
			//if the directory exists, we return an error
			client.requestStatus("createDirectory", path, false, -1);
		}
		else{
			for(int CS = 1; CS <= chunkservers.size(); CS++){
				chunkservers.get(CS).createFile(path);
			}
			client.requestStatus("createDirectory", path, true, -1);
		}
	}
	
	public void deleteDirectoryA(String path, int clientID) throws RemoteException
	{
		Node file = directory.root.find(directory.pathTokenizer(path),1);
		if(file == null){
			System.err.println("Nonexistent directory " + path);
			client.requestStatus("deleteDirectory", path, false, -1);
		}
		else if(directory.removeElement(directory.pathTokenizer(path)))
		{
			for(Integer CS : file.chunkServersNum){
				chunkservers.get(CS).deleteDirectory(path);
			}
			client.requestStatus("deleteFile", path, true, -1);
		}
		else
		{
			System.out.println("Delete unsuccessful. Item not found.");
			client.requestStatus("deleteDirectory", path, false, -1);
		}		
	}
	
	public void appendA(String chunkhandle, int clientID) throws RemoteException
	{
		Node file = directory.root.find(directory.pathTokenizer(chunkhandle), 1);
		if(file == null)
		{
			client.requestStatus("append", chunkhandle, false, -1);
		}
		else{
			client.passMetaData(chunkhandle, -1, file.chunkServersNum);
		}
	}
	
	public void atomicAppendA(String chunkhandle, int clientID) throws RemoteException
	{
		Node file = directory.root.find(directory.pathTokenizer(chunkhandle), 1);
		if(file == null)
		{
			client.requestStatus("atomicAppend", chunkhandle, false, -1);
		}
		else if (file.getPrimaryLeaseTime() < (System.currentTimeMillis() - MINUTE))
		{
			client.passMetaData(chunkhandle, file.getPrimaryChunkserver(), file.chunkServersNum);
		}
		else
		{
			Random randInt = new Random();
			int randomCS = randInt.nextInt() % file.chunkServersNum.size();
			file.issuePrimaryLease(file.chunkServersNum.get(randomCS), System.currentTimeMillis());
			client.passMetaData(chunkhandle, file.getPrimaryChunkserver(), file.chunkServersNum);
		}	
	}

	public void heartbeatA(int CSID) throws RemoteException
	{
		//this function is called when the chunkserver comes back online and an update is required
		chunkservers.get(CSID).refreshMetadata();
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

