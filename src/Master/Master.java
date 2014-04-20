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
import Interfaces.MasterInterface;

public class Master implements MasterInterface{

	final static String NOT_FOUND ="Sorry, but the file you had requesting was not found";
	final static long MINUTE = 60000;
	Map<String,Metadata> files;
	Tree directory;
	Semaphore stateChange;
	private MasterThread masterThread;
	List<Task> tasks;
	public enum TaskType {createF, deleteF, createD, deleteD, append, aAppend, heartbeat};
	ClientInterface client;
	HashMap<Integer, ChunkserverInterface> chunkservers;

	public Master() {
		directory = new Tree();
		chunkservers = new HashMap<Integer, ChunkserverInterface>();
		files = Collections.synchronizedMap(new HashMap<String,Metadata>());
		stateChange = new Semaphore(1, true); // binary semaphore
		tasks = Collections.synchronizedList(new ArrayList<Task>());
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
	
	/*
	 * Messages from Clients or Chunkservers
	 * */
	
	public void createFile(String path, String fileName, int numReplicas,
			int clientID) throws RemoteException {
		tasks.add(new Task(TaskType.createF,path,fileName,numReplicas,clientID));
		stateChanged();
	}

	public void deleteFileMaster(String chunkhandle, int clientID)
			throws RemoteException {
		tasks.add(new Task(TaskType.deleteF,chunkhandle,clientID));
		stateChanged();
	}

	public void createDirectory(String path, int clientID)
			throws RemoteException {
		tasks.add(new Task(TaskType.createD,path,clientID));
		stateChanged();
	}
	public void deleteDirectory(String path, int clientID)
			throws RemoteException {
		tasks.add(new Task(TaskType.deleteD,path,clientID));
		stateChanged();
	}

	public void append(String chunkhandle, int clientID) throws RemoteException {
		tasks.add(new Task(TaskType.append,chunkhandle,clientID));
		stateChanged();
	}

	public void atomicAppend(String chunkhandle, int clientID)
			throws RemoteException {
		tasks.add(new Task(TaskType.aAppend,chunkhandle,clientID));
		stateChanged();
	}

	public void heartbeat(int CSID) throws RemoteException {
		tasks.add(new Task(TaskType.heartbeat,CSID));
		stateChanged();
	}

	protected boolean pickAndExecuteAnAction(){
		// scheduler checks, return true if something to do
		if(tasks.size() != 0){
			try{
			if(tasks.get(0).getType() == TaskType.createF){
				createFileA(tasks.get(0).getPath(), tasks.get(0).getFileName(), tasks.get(0).getNumReplicas(), tasks.get(0).getClientID());
				tasks.remove(0);
				return true;
			}
			else if(tasks.get(0).getType() == TaskType.deleteF){
				deleteFileMasterA(tasks.get(0).getPath(), tasks.get(0).getClientID());
				tasks.remove(0);
				return true;
			}
			else if(tasks.get(0).getType() == TaskType.createD){
				createDirectoryA(tasks.get(0).getPath(), tasks.get(0).getClientID());
				tasks.remove(0);
				return true;
			}
			else if(tasks.get(0).getType() == TaskType.deleteD){
				deleteDirectoryA(tasks.get(0).getPath(), tasks.get(0).getClientID());
				tasks.remove(0);
				return true;
			}
			else if(tasks.get(0).getType() == TaskType.append){
				appendA(tasks.get(0).getPath(), tasks.get(0).getClientID());
				tasks.remove(0);
				return true;
			}
			else if(tasks.get(0).getType() == TaskType.aAppend){
				atomicAppendA(tasks.get(0).getPath(), tasks.get(0).getClientID());
				tasks.remove(0);
				return true;
			}
			else if(tasks.get(0).getType() == TaskType.heartbeat){
				heartbeatA(tasks.get(0).getCSID());
				tasks.remove(0);
				return true;
			}
			}catch (RemoteException e){
				System.out.println("Remote Exception connecting from Master.");
				e.printStackTrace();
			}
		}
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
	
	// Master creates a Task when receiving a message from a client or CS
	// Master queues Tasks and uses this class to call actions 
	private class Task{
		String path;	// same as chunkhandle for some calls
		String fileName;
		int numReplicas;
		int CSID;
		int clientID;
		TaskType type;
		
		// called by createFile
		public Task(TaskType type, String path, String fileName, int numReplicas, int clientID){
			this.type = type;
			this.path = path;
			this.fileName = fileName;
			this.numReplicas = numReplicas;
			this.clientID = clientID;
		}
		
		// called by deleteFile, append, createDirectory, deleteDirectory, and atomicAppend 
		public Task(TaskType type, String path, int clientID){
			this.type = type;
			this.path = path;
			this.clientID = clientID;
		}
		
		// called by heartbeat
		public Task(TaskType type, int CSID){
			this.type = type;
			this.CSID = CSID;
		}

		// Getters
		public int getClientID() {
			return clientID;
		}
		public int getCSID() {
			return CSID;
		}
		public String getFileName() {
			return fileName;
		}
		public int getNumReplicas() {
			return numReplicas;
		}
		public String getPath() {
			return path;
		}
		public TaskType getType() {
			return type;
		}
	}
}

