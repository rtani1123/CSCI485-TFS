package Master;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.Naming;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
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
import Utilities.Storage;
import Interfaces.MasterInterface;

public class Master extends UnicastRemoteObject implements MasterInterface{

	final static String NOT_FOUND ="Sorry, but the file you had requesting was not found";
	final static long MINUTE = 60000;
	Tree directory;
	Semaphore stateChange;
	private MasterThread masterThread;
	List<Task> tasks;
	public enum TaskType {createF, deleteF, createD, deleteD, read, append, aAppend, heartbeat};
	ClientInterface client;
	HashMap<Integer, ChunkserverInterface> chunkservers;

	public Master() throws RemoteException{
		directory = new Tree();
		chunkservers = new HashMap<Integer, ChunkserverInterface>();
		stateChange = new Semaphore(1, true); // binary semaphore
		tasks = Collections.synchronizedList(new ArrayList<Task>());
		startThread();

		setupMasterHost();
		
		connectToClient();
		try {
			client.connectToMaster();
		} catch(RemoteException re) {
			System.out.println("Cannot connect to client");
		}
		//client.createDirectory("C:/1");
	}

	/**
	 * RMI Connection Code
	 */

	/**
	 * The format for connection should be "rmi:DOMAIN/ChunkserverID".  
	 * ChunkserverID will be different for each instance of Chunkserver.
	 * MasterCS is NOT the same as CSMaster.  
	 * MasterCS -> Master calls CS functions;  CS is host of functions.
	 *
	 * For this, the chunkserver is hosted on dblab-18.
	 */
	//Chunkserver calls Master methods -> CHUNKMASTER1
	public void setupMasterHost() {
		try {
			System.setSecurityManager(new RMISecurityManager());
			Registry registry = LocateRegistry.createRegistry(1099);
			Naming.rebind("rmi://dblab-18.vlab.usc.edu/MASTER", this);
			System.out.println("Master Host Setup Success");
		} catch (MalformedURLException re) {
			System.out.println("Bad connection");
			re.printStackTrace();
		} catch (RemoteException e) {
			System.out.println("Bad connection");
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	//Master calls Chunkserver methods -> MASTERCHUNK1
	public void connectToChunkserver(Integer index) {
		try {
			System.setSecurityManager(new RMISecurityManager());
			ChunkserverInterface tempCS;
			System.out.println( "attempting connect to: dblab-36.vlab.usc.edu:123/CHUNK" + index.toString());
			tempCS = (ChunkserverInterface)Naming.lookup("rmi://dblab-36.vlab.usc.edu:123/CHUNK" + index.toString());
			
			//TODO: Change this to handle multiple chunkservers.
			chunkservers.put(index, tempCS);
			/*
			 * ChunkServer FUNCTION HOST implementation
			 */
			System.out.println("Connection to Chunkserver " + index + " Success" );

		} catch(Exception re) {
			re.printStackTrace();
		}
	}
	
	//Master calls Client methods -> MASTERCLIENT
	public void connectToClient() {
		try {
			System.setSecurityManager(new RMISecurityManager());
			
			client = (ClientInterface)Naming.lookup("rmi://dblab-43.vlab.usc.edu/CLIENT");
			System.out.println("Connection to Client Success");

		} catch(Exception re) {
			re.printStackTrace();
		}
	}

	/**
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

	/**
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

	public void append(String chunkhandle, int clientID, int reqID) throws RemoteException {
		tasks.add(new Task(TaskType.append,chunkhandle,clientID, reqID));
		stateChanged();
	}

	public void atomicAppend(String chunkhandle, int clientID, int reqID)
			throws RemoteException {
		tasks.add(new Task(TaskType.aAppend,chunkhandle,clientID,reqID));
		stateChanged();
	}

	public void read(String chunkhandle, int clientID, int reqID) throws RemoteException{
		tasks.add(new Task(TaskType.read,chunkhandle,clientID,reqID));
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
					appendA(tasks.get(0).getPath(), tasks.get(0).getClientID(), tasks.get(0).getReqID());
					tasks.remove(0);
					return true;
				}
				else if(tasks.get(0).getType() == TaskType.aAppend){
					atomicAppendA(tasks.get(0).getPath(), tasks.get(0).getClientID(), tasks.get(0).getReqID());
					tasks.remove(0);
					return true;
				}
				else if(tasks.get(0).getType() == TaskType.read){
					readA(tasks.get(0).getPath(), tasks.get(0).getClientID(), tasks.get(0).getReqID());
					tasks.remove(0);
					return true;
				}
				else if(tasks.get(0).getType() == TaskType.heartbeat){
					//TODO: send a heartbeat message
					tasks.remove(0);
					return true;
				}
			}
			catch (RemoteException e){
				System.out.println("Remote Exception connecting from Master.");
				e.printStackTrace();
			}
		}
		return false;
	}

	/**
	 * Function createFileA creates a new file in the master namespace.
	 * It is exclusively a handshake and does not transfer data, although it does result in
	 * creation of the file on the chunkservers. The default number of replicas is three (one copy on each
	 * chunkserver), and the maximum number of replicas is also 3.
	 * @param path
	 * @param fileName
	 * @param numReplicas
	 * @param clientID
	 * @throws RemoteException
	 */
	public void createFileA(String path, String fileName, int numReplicas, int clientID) throws RemoteException
	{
		directory.getAllPath(directory.root);
		Node file = directory.root.find(directory.pathTokenizer(path + "/" + fileName), 1);	
		if (file != null){
			//if the file exists, we return an error
			System.err.println("Error file already exists.");
			try{
				client.requestStatus("createFile", path + "/" + fileName, false, -1);
			}
			catch (RemoteException re){
				System.out.println ("Connection failure to client.");
			}
		}
		else{
			//pick the number of replicas
			ArrayList<Integer> CSLocations = new ArrayList<Integer>();
			CSLocations.add(1);
//			if (numReplicas == 3){
//				//all of the chunkservers get the file
//				CSLocations.add(1);
//				CSLocations.add(2);
//				CSLocations.add(3);
//			}
//			else{
//			//TODO: set up a random way to pick up chunkservers
//			}
			System.out.println(path+"/"+fileName);
			if(directory.addElement(directory.pathTokenizer(path+"/"+fileName),CSLocations)){
				System.out.println("Successful add to tree. Requesting file create from CS.");
				for(Integer CS : CSLocations){
					try{
						chunkservers.get(CS).createFile(path + "/" + fileName);
					}
					catch(RemoteException re){
						System.out.println("Error connecting to chunkserver " + CS);
						//TODO: handle rolling back the write
					}
				}
//				try{
//					client.requestStatus("createFile", path + "/" + fileName, true, -1);
//				}
//				catch(RemoteException re){
//					System.out.println("Error connecting to client.");
//				}
			}
			else{
				try{
					System.out.println("Element addition to file system failed. Invalid path.");
					client.requestStatus("createFile", path + "/" + fileName, false, -1);
				}
				catch(RemoteException re){
					System.out.println("Connection to client failed.");
				}
			}
		}
		Storage.storeTree(directory);
	}

	/**
	 * The function deleteFileMasterA deletes all metadata for a given file from the master.
	 * It also orders the chunkservers storing the file itself to remove the file and its 
	 * corresponding metadata.
	 * @param chunkhandle
	 * @param clientID
	 * @throws RemoteException
	 */
	public void deleteFileMasterA(String chunkhandle, int clientID) throws RemoteException
	{
		Node file = directory.root.find(directory.pathTokenizer(chunkhandle),1);
		if(file == null){
			System.err.println("Nonexistent path " + chunkhandle);
			try{
				client.requestStatus("deleteFile", chunkhandle, false, -1);
			}
			catch(RemoteException re){
				System.out.println("Error connecting to client.");
			}
		}
		else if(directory.removeElement(directory.pathTokenizer(chunkhandle)))
		{
			for(Integer CS : file.chunkServersNum){
				try{
					System.err.println("Messaging chunkserver " + CS);
					chunkservers.get(CS).deleteFile(chunkhandle);
				}
				catch(RemoteException re){
					System.out.println("Error connecting to chunkserver " + CS);
					//TODO: handle rolling back of the remove request?
				}
			}
//			try{
//				client.requestStatus("deleteFile", chunkhandle, true, -1);
//			}
//			catch(RemoteException re){
//				System.out.println("Error connecting to client.");
//			}
			System.err.println("file removal success.");
		}
		else
		{
			System.out.println("Delete unsuccessful. Item not found.");
			try{
				client.requestStatus("deleteFile", chunkhandle, false, -1);
			}
			catch(RemoteException re){
				System.out.println("Error connecting to client.");
			}
		}
		Storage.storeTree(directory);
	}

	/**
	 * The function createDirectoryA creates a directory within the master namespace.
	 * It also orders for the directory to be created on all chunkservers.
	 * @param path
	 * @param clientID
	 * @throws RemoteException
	 */
	public void createDirectoryA(String path, int clientID) throws RemoteException{
		Node newDir = directory.root.find(directory.pathTokenizer(path), 1);	
		if (newDir != null){
			//if the directory exists, we return an error
			try{
				client.requestStatus("createDirectory", path, false, -1);
			}
			catch(RemoteException re){
				System.out.println("Error connecting to client.");
			}
		}
		else{
			ArrayList<Integer> chunkserversL = new ArrayList<Integer>();
			//TODO: add all chunkservers to this array list
			chunkserversL.add(1);
			if(directory.addElement(directory.pathTokenizer(path),chunkserversL)){
				for(int CS = 1; CS <= chunkservers.size(); CS++){
					try{
						chunkservers.get(CS).createDirectory(path);
					}
					catch(RemoteException re){
						System.out.println("Error connecting to chunkserver " + CS);
					}
				}
				//			try{
				//				client.requestStatus("createDirectory", path, true, -1);
				//			}
				//			catch(RemoteException re){
				//				System.out.println("Error connecting to client.");
				//			}
			}
			else{
				//TODO: Handle the failed add
				System.err.println("Directory add to tree failure.");
			}
		}
		Storage.storeTree(directory);
	}

	/**
	 * The function deleteDirectoryA immediately deletes the given directory and
	 * all of its contents from both the master namespace and any replicas that 
	 * store it.
	 * @param path
	 * @param clientID
	 * @throws RemoteException
	 */
	public void deleteDirectoryA(String path, int clientID) throws RemoteException
	{
		Node file = directory.root.find(directory.pathTokenizer(path),1);
		if(file == null){
			System.err.println("Nonexistent directory " + path);
			try{
				client.requestStatus("deleteDirectory", path, false, -1);
			}
			catch(RemoteException re){
				System.out.println("Error connecting to client.");
			}
			return;
		}
		if(directory.removeElement(directory.pathTokenizer(path)))
		{
			for(Integer CS : file.chunkServersNum){
				try{
					chunkservers.get(CS).deleteDirectory(path);
				}
				catch(RemoteException re){
					//TODO: Handle rolling back of the request in case of no success?
					System.out.println("Error connecting to chunkserver " + CS);
				}
			}
//			try{
//				client.requestStatus("deleteFile", path, true, -1);
//			}
//			catch(RemoteException re){
//				System.out.println("Error connecting to client.");
//			}
			System.err.println("Directory deletion successful.");
		}
		else
		{
			System.out.println("Delete unsuccessful. Item not found.");
			try{
				client.requestStatus("deleteDirectory", path, false, -1);
			}
			catch(RemoteException re){
				System.out.println("Error connecting to client.");
			}
		}
		Storage.storeTree(directory);
	}

	/**
	 * The appendA function returns metadata sufficient for an append to the client
	 * that has requested it. The parameter for primaryLease passed to the client is 
	 * null because no primary lease is required.
	 * @param chunkhandle
	 * @param clientID
	 * @throws RemoteException
	 */
	public void appendA(String chunkhandle, int clientID, int reqID) throws RemoteException
	{
		System.out.println("Attempt action append");
		Node file = directory.root.find(directory.pathTokenizer(chunkhandle), 1);
		System.out.println("Attempt action append");
		if(file == null)
		{
			try{
				client.requestStatus("append", chunkhandle, false, -1);
			}
			catch(RemoteException re){
				System.out.println("Error connecting to client.");
			}
		}
		else{
			try{
				client.passMetaData(chunkhandle, -1, file.chunkServersNum, reqID);
			}
			catch(RemoteException re){
				System.out.println("Error connecting to client.");
			}
		}
	}

	/**
	 * The readA function sends metadata for the appropriate chunkhandle to the 
	 * client that requested it. Like append, it sends a null parameter for the primary
	 * lease chunkserver.
	 * @param chunkhandle
	 * @param clientID
	 * @throws RemoteException
	 */
	public void readA(String chunkhandle, int clientID, int reqID) throws RemoteException {
		Node file = directory.root.find(directory.pathTokenizer(chunkhandle), 1);
		if(file == null)
		{
			try{
				client.requestStatus("read", chunkhandle, false, -1);
			}
			catch(RemoteException re){
				System.out.println("Error connecting to client.");
			}
		}
		else{
			try{
				client.passMetaData(chunkhandle, -1, file.chunkServersNum, reqID);
			}
			catch(RemoteException re){
				System.out.println("Error connecting to client.");
			}
		}	
	}

	/**
	 * The function atomicAppendA sends the client in clientID the metadata required
	 * for an atomic append. If a primary lease already exists, it sends that data
	 * to the client. If the primary lease does not exist, it picks a random chunkserver
	 * that is storing the replica for the file and issues a primary lease before 
	 * messaging the client.
	 * @param chunkhandle
	 * @param clientID
	 * @throws RemoteException
	 */
	public void atomicAppendA(String chunkhandle, int clientID, int reqID) throws RemoteException
	{
		Node file = directory.root.find(directory.pathTokenizer(chunkhandle), 1);
		if(file == null)
		{
			try{
				client.requestStatus("atomicAppend", chunkhandle, false, -1);
			}
			catch(RemoteException re){
				System.out.println("Error connecting to client.");
			}
		}
		else if (file.getPrimaryLeaseTime() < (System.currentTimeMillis() - MINUTE))
		{
			try{
				client.passMetaData(chunkhandle, file.getPrimaryChunkserver(), file.chunkServersNum, reqID);
			}
			catch(RemoteException re){
				System.out.println("Error connecting to client.");
			}
		}
		else
		{
			Random randInt = new Random();
			int randomCS = randInt.nextInt() % file.chunkServersNum.size();
			file.issuePrimaryLease(file.chunkServersNum.get(randomCS), System.currentTimeMillis());
			try{
				client.passMetaData(chunkhandle, file.getPrimaryChunkserver(), file.chunkServersNum, reqID);
			}
			catch(RemoteException re){
				System.out.println("Error connecting to client.");
			}
			try{
				chunkservers.get(randomCS).primaryLease(chunkhandle, new ArrayList<Integer>());
			}
			catch(RemoteException re){
				//TODO: Assign the primary lease to a different chunkserver
				System.out.println("Error connecting to chunkserver " + randomCS);
			}
		}	
	}

	public void restoreChunkserver(int CSID) throws RemoteException {

	}
	
	public void createDirectoryRedo(String path, int chunkserverID) throws RemoteException {
		
	}
	
	public void createFileRedo(String chunkhandle, int chunkserverID) throws RemoteException {
		
	}
	
	public void deleteFileRedo(String chunkhandle, int chunkserverID) throws RemoteException {
		
	}
	
	public void deleteDirectoryRedo(String chunkhandle, int chunkserverID) throws RemoteException {
		
	}
	
	public void fetchAndRewrite(String chunkhandle, int CSSource, int chunkserverID) throws RemoteException {
		
	}

	public static void main(String[] args) {
		try {
			Master master = new Master();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Internal class to handle the thread within master to run the scheduler.
	 * 
	 */
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

	/**
	 * The Task class is used for master to keep track of what is necessary
	 * to process a request.
	 * Master creates a Task when receiving a message from a client or CS
	 * Master queues Tasks and uses this class to call actions 
	 * 
	 */
	private class Task{
		String path;	// same as chunkhandle for some calls
		String fileName;
		int numReplicas;
		int CSID;
		int reqID;
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

		// called by read, append, and atomicAppend 
		public Task(TaskType type, String path, int clientID, int reqID){
			this.type = type;
			this.path = path;
			this.clientID = clientID;
			this.reqID = reqID;
		}

		// called by deleteFile, createDirectory, deleteDirectory
		public Task(TaskType type, String path, int clientID){
			this.type = type;
			this.path = path;
			this.clientID = clientID;
			this.reqID = -1;
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
		public int getReqID(){
			return reqID;
		}
	}

	
	/**
	 * Class CSInfo is used by master to maintain metadata for connecting to various
	 * chunkservers. It keeps track of the most recent heartbeat time received from
	 * the chunkserver, the status of the chunkserver, and the chunkserver ID.
	 * @author lazzarid
	 *
	 */
	public enum CSStatus {DOWN, UNUSED, OK, RECOVERING};
	private class CSInfo{
		CSStatus status;
		long lastHeartbeat;
		int id;
		ChunkserverInterface remoteCS;
		
		public CSInfo(ChunkserverInterface csi, int id){
			this.id = id;
			this.remoteCS = csi;
			this.lastHeartbeat = System.currentTimeMillis();
			this.status = CSStatus.OK;
		}
		
		public CSStatus getStatus(){
			return status;
		}
		
		public long getLastHB(){
			return lastHeartbeat;
		}
		
		public ChunkserverInterface getCS(){
			return remoteCS;
		}
		
		public int getID(){
			return id;
		}
		
		public void setStatus(CSStatus status){
			this.status = status;
		}

		public void setLastHB(long LHB){
			this.lastHeartbeat = LHB;
		}
	}
}

