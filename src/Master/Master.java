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
import java.util.ConcurrentModificationException;
//import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Semaphore;

import Interfaces.ChunkserverInterface;
import Interfaces.ClientInterface;
import Utilities.Node;
import Utilities.OperationsLog;
import Utilities.Tree;
import Utilities.Storage;
import Interfaces.MasterInterface;

public class Master extends UnicastRemoteObject implements MasterInterface{

	final static String NOT_FOUND ="Sorry, but the file you had requesting was not found";
	final static long MINUTE = 60000;
	final static long HEARTBEAT_DELAY = 5000;
	public static Tree directory;
	OperationsLog log;
	Semaphore stateChange;
	private MasterThread masterThread;
	List<Task> tasks;
	public enum TaskType {recoverCS, createF, deleteF, createD, deleteD, read, append, aAppend};
	//ClientInterface client;
	Map<Integer, ClientInterface> clients;
	Map<Integer, CSInfo> chunkservers;
	Heartbeat heartbeat;

	public Master() throws RemoteException{
		if(Storage.getTree() != null) {
			directory = Storage.getTree();
			log = Storage.getLog();
		}
		else {
			directory = new Tree();
			log = new OperationsLog();
		}
		
		chunkservers = Collections.synchronizedMap(new HashMap<Integer, CSInfo>());
		clients = Collections.synchronizedMap(new HashMap<Integer, ClientInterface>());
		stateChange = new Semaphore(1, true); // binary semaphore
		tasks = Collections.synchronizedList(new ArrayList<Task>());
		heartbeat = new Heartbeat();
		heartbeat.start();		// initiate run method in Heartbeat class, start sending out heartbeat messages
		startThread();		
		setupMasterHost();

		try {
			connectToClient(11);
		} catch(Exception e) {
			System.out.println("Cannot connect to client");
		}
		try {
			connectToChunkserver(1);
			chunkservers.get(1).getCS().connectToMaster();
		}
		catch(Exception e) {
			System.out.println("Chunkserver 1 not running.");
		}
		try {
			connectToChunkserver(2);
			chunkservers.get(2).getCS().connectToMaster();
		}
		catch(Exception e) {
			System.out.println("Chunkserver 2 not running.");
		}
		try {
			connectToChunkserver(3);
			chunkservers.get(3).getCS().connectToMaster();
		}
		catch(Exception e) {
			System.out.println("Chunkserver 3 not running.");
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
			System.out.println("Bad connection - MalformedURLException");
		} catch (RemoteException e) {
			System.out.println("Bad connection - RemoteException");
		} catch (Exception e) {
			System.out.println("Bad connection - Misc Exception");
		}
	}
	//Master calls Chunkserver methods -> MASTERCHUNK1
	public void connectToChunkserver(Integer index) {
		try {
			//connects existing chunkservers to new chunkserver.
			for(Map.Entry<Integer, CSInfo> entry : chunkservers.entrySet()) {
				if(!(entry.getKey().equals(index)) && entry.getValue().getStatus() != CSStatus.DOWN) {
					((CSInfo)entry.getValue()).getCS().connectToChunkserver(index);
				}
			}
			System.setSecurityManager(new RMISecurityManager());
			ChunkserverInterface tempCS = null;
			if(index == 1)
				tempCS = (ChunkserverInterface)Naming.lookup("rmi://dblab-36.vlab.usc.edu:123/CHUNK" + index.toString());
			else if(index == 2)
				tempCS = (ChunkserverInterface)Naming.lookup("rmi://dblab-05.vlab.usc.edu:124/CHUNK" + index.toString());
			else if(index == 3)
				tempCS = (ChunkserverInterface)Naming.lookup("rmi://dblab-29.vlab.usc.edu:125/CHUNK" + index.toString());
		
			if(chunkservers.get(index) == null) {
				System.out.println("doesn't exist. creating new");
				CSInfo temp = new CSInfo(tempCS, index);
				chunkservers.put(index, temp);
			}
			else {
				System.out.println("updating cs interface " + index);
				chunkservers.get(index).setCS(tempCS);
				chunkservers.get(index).setStatus(CSStatus.DOWN);
			}
			for(Map.Entry<Integer, CSInfo> entry : chunkservers.entrySet()) {
				if(entry.getValue().id != index) {
					tempCS.connectToChunkserver(entry.getKey());
				}
			}
			
			/*
			 * ChunkServer FUNCTION HOST implementation
			 */
			System.out.println("Connection to Chunkserver " + index + " Success" );

		} catch(Exception re) {
			System.out.println("Error in connect to Chunkserver " + index);
		}
	}

	//Master calls Client methods -> MASTERCLIENT
	public void connectToClient(Integer index) {
		try {
			ClientInterface client = (ClientInterface)Naming.lookup("rmi://dblab-43.vlab.usc.edu:"+index +"/CLIENT"+index);
			clients.put(index, client);
			System.out.println("Connection to Client " + index + " Success");
			HashMap<Integer, ChunkserverInterface> csTemp = new HashMap<Integer, ChunkserverInterface>();
			for(Map.Entry<Integer, CSInfo> entry : chunkservers.entrySet()) {
				if(entry.getValue().getStatus() == CSStatus.OK)
					csTemp.put(entry.getKey(), entry.getValue().getCS());
			}
			client.setChunkservers(csTemp);
			client.connectToMaster();
		} catch(Exception re) {
			System.out.println("Error in connection to client");
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
		log.makeLogRecord(System.currentTimeMillis(), path + "/" + fileName, "createFile", 0);
		System.out.println("Master received createFile request for " + path + "/" + fileName);
		stateChanged();
	}

	public void deleteFileMaster(String chunkhandle, int clientID)
			throws RemoteException {
		tasks.add(new Task(TaskType.deleteF,chunkhandle,clientID));
		log.makeLogRecord(System.currentTimeMillis(), chunkhandle, "deleteFile", 0);
		System.out.println("Master received deleteFile request for " + chunkhandle);
		stateChanged();
	}

	public void createDirectory(String path, int clientID)
			throws RemoteException {
		tasks.add(new Task(TaskType.createD,path,clientID));
		log.makeLogRecord(System.currentTimeMillis(), path, "createDirectory", 0);
		System.out.println("Master received createDirectory request for " + path);
		stateChanged();
	}
	public void deleteDirectory(String path, int clientID)
			throws RemoteException {
		tasks.add(new Task(TaskType.deleteD,path,clientID));
		log.makeLogRecord(System.currentTimeMillis(), path, "deleteDirectory", 0);
		System.out.println("Master received deleteDirectory request for " + path);
		stateChanged();
	}

	public void append(String chunkhandle, int clientID, int reqID) throws RemoteException {
		tasks.add(new Task(TaskType.append,chunkhandle,clientID, reqID));
		log.makeLogRecord(System.currentTimeMillis(), chunkhandle, "append", 0);
		System.out.println("Master received request for append metadata for " + chunkhandle);
		stateChanged();
	}

	public void atomicAppend(String chunkhandle, int clientID, int reqID)
			throws RemoteException {
		tasks.add(new Task(TaskType.aAppend,chunkhandle,clientID,reqID));
		log.makeLogRecord(System.currentTimeMillis(), chunkhandle, "atomicAppend", 0);
		System.out.println("Master received request for atomicAppend metadata for " + chunkhandle);
		stateChanged();
	}

	public void read(String chunkhandle, int clientID, int reqID) throws RemoteException{
		tasks.add(new Task(TaskType.read,chunkhandle,clientID,reqID));
		log.makeLogRecord(System.currentTimeMillis(), chunkhandle, "read", 0);
		System.out.println("Master received request for read metadata for " + chunkhandle);
		stateChanged();
	}

	public void heartbeat(int CSID) throws RemoteException {
		chunkservers.get(CSID).setLastHB(System.currentTimeMillis());
		if(chunkservers.get(CSID).getStatus() == CSStatus.OK){
			//if the chunkserver was up to date, assume it is still up to date
			chunkservers.get(CSID).setLastGoodTime(System.currentTimeMillis());
		}
		System.out.println("Master received heartbeat response from " + CSID);
	}

	/**
	 * Scheduler
	 * @return
	 */
	protected boolean pickAndExecuteAnAction(){
		// scheduler checks, return true if something to do
		if(tasks.size() != 0){
			try{
				if(tasks.get(0).getType() == TaskType.recoverCS){
					restoreChunkserver(tasks.get(0).getCSID());
					tasks.remove(0);
					return true;
				}
				else if(tasks.get(0).getType() == TaskType.createF){
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
			}
			catch (ConcurrentModificationException cme) {
				System.out.println("Concurrent Modificiation Exception in Master Scheduler");
				cme.printStackTrace();
				return true;
			}
			catch (RemoteException e){
				System.out.println("Remote Exception connecting from Master.");
			}
		}
		return false;
	}

	/**
	 * Action functions for the Master
	 */
	
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
			System.out.println("File creation failed. " + path + "/" + fileName + " already exists.");
			try{
				clients.get(clientID).requestStatus("createFile", path + "/" + fileName, false, -1);
			}
			catch (RemoteException re){
				System.out.println ("Connection failure to client.");
			}
		}
		else{
			//pick the number of replicas
			ArrayList<Integer> availableCS = new ArrayList<Integer>();
			ArrayList<Integer> CSLocations = new ArrayList<Integer>();
			for(Map.Entry<Integer, CSInfo> entry : chunkservers.entrySet()){
				availableCS.add(entry.getKey());
			}
			if (numReplicas > availableCS.size()){
				System.out.println("Error. Number of replicas exceeds number of chunkservers.");
				try{
					clients.get(clientID).requestStatus("createFile", path + "/" + fileName, false, -1);
				}
				catch(RemoteException re){
					System.out.println("Connection to client failed.");
				}
				return;
			}
			else if (numReplicas == availableCS.size()){
				for(Integer i : availableCS){
					CSLocations.add(i);
				}
			}
			else{
				int count = 0;
				while (count < numReplicas){					
					Random rand = new Random();
					int randIndex = Math.abs((rand.nextInt() % availableCS.size()));
					if(!CSLocations.contains(availableCS.get(randIndex))){
						CSLocations.add(availableCS.get(randIndex));
						count++;
					}
				}
			}
			System.out.println(path+"/"+fileName);
			if(directory.addElement(directory.pathTokenizer(path+"/"+fileName),CSLocations)){
				log.makeLogRecord(System.currentTimeMillis(), path+"/"+fileName, "createFile", 1);
				Storage.storeLog(log);
				System.out.println("Successful add to tree. Requesting file create from CS.");
				int downChunkservers = 0;
				for(Integer CS : CSLocations){
					try{
						chunkservers.get(CS).getCS().createFile(path + "/" + fileName);
						chunkservers.get(CS).setLastGoodTime(System.currentTimeMillis());
					}
					catch(RemoteException re){
						System.out.println("Error connecting to chunkserver " + CS);
						chunkservers.get(CS).setStatus(CSStatus.DOWN);
						downChunkservers++;
					}
				}
				if(downChunkservers == numReplicas){
					System.out.println("Element addition to file system failed. No available chunkservers.");
					try{
						clients.get(clientID).requestStatus("createFile", path + "/" + fileName, false, -1);
					}
					catch(RemoteException re){
						System.out.println("Connection to client failed.");
					}					
				}
				else{
					try{
						clients.get(clientID).requestStatus("createFile", path + "/" + fileName, true, -1);
					}
					catch(RemoteException re){
						System.out.println("Error connecting to client.");
					}
					System.out.println("Successful addition of file to file system");
				}
			}
			else{
				try{
					System.out.println("Element addition to file system failed. Invalid path.");
					clients.get(clientID).requestStatus("createFile", path + "/" + fileName, false, -1);
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
			System.out.println("Cannot delete nonexistent path " + chunkhandle);
			try{
				clients.get(clientID).requestStatus("deleteFile", chunkhandle, false, -1);
			}
			catch(RemoteException re){
				System.out.println("Error connecting to client.");
			}
		}
		else if(directory.removeElement(directory.pathTokenizer(chunkhandle)))
		{
			log.makeLogRecord(System.currentTimeMillis(), chunkhandle, "deleteFile", 1);
			Storage.storeLog(log);
			for(Integer CS : file.chunkServersNum){
				try{
					System.err.println("Messaging chunkserver " + CS);
					chunkservers.get(CS).getCS().deleteFile(chunkhandle);
					chunkservers.get(CS).setLastGoodTime(System.currentTimeMillis());
				}
				catch(RemoteException re){
					System.out.println("Error connecting to chunkserver " + CS);
					chunkservers.get(CS).setStatus(CSStatus.DOWN);
				}
			}
			try{
				clients.get(clientID).requestStatus("deleteFile", chunkhandle, true, -1);
			}
			catch(RemoteException re){
				System.out.println("Error connecting to client.");
			}
			System.out.println("File successfully removed from file system.");
			
		}
		else
		{
			System.out.println("Delete unsuccessful. Item not found.");
			try{
				clients.get(clientID).requestStatus("deleteFile", chunkhandle, false, -1);
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
			System.out.println("Error. Directory creation failed. " + path + " already exists");
			try{
				clients.get(clientID).requestStatus("createDirectory", path, false, -1);
			}
			catch(RemoteException re){
				System.out.println("Error connecting to client.");
			}
		}
		else{
			ArrayList<Integer> chunkserversL = new ArrayList<Integer>();
			for(Map.Entry<Integer, CSInfo> entry: chunkservers.entrySet()){
				chunkserversL.add(entry.getKey());
			}
			if(directory.addElement(directory.pathTokenizer(path),chunkserversL)){
				log.makeLogRecord(System.currentTimeMillis(), path, "createDirectory", 1);
				Storage.storeLog(log);
				for(Map.Entry<Integer, CSInfo> entry: chunkservers.entrySet()){
					if(entry.getValue().getStatus() == CSStatus.OK){
						try{
							entry.getValue().getCS().createDirectory(path);
							entry.getValue().setLastGoodTime(System.currentTimeMillis());
						}
						catch(RemoteException re){
							System.out.println("Error connecting to chunkserver " + entry.getKey());
							entry.getValue().setStatus(CSStatus.DOWN);
						}
					}
				}
				try{
					clients.get(clientID).requestStatus("createDirectory", path, true, -1);
				}
				catch(RemoteException re){
					System.out.println("Error connecting to client.");
				}
				System.out.println("Successful directory creation " + path);
			}
			else{
				try{
					clients.get(clientID).requestStatus("createDirectory", path, false, -1);
				}
				catch(RemoteException re){
					System.out.println("Error connecting to client.");
				}
				System.out.println("Directory add to tree failure.");
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
				clients.get(clientID).requestStatus("deleteDirectory", path, false, -1);
			}
			catch(RemoteException re){
				System.out.println("Error connecting to client.");
			}
			return;
		}
		if(directory.removeElement(directory.pathTokenizer(path)))
		{
			log.makeLogRecord(System.currentTimeMillis(), path, "deleteDirectory", 1);
			Storage.storeLog(log);
			System.out.println("Directory removed from namespace " + path);
			for(Integer CS : file.chunkServersNum){
				try{
					chunkservers.get(CS).getCS().deleteDirectory(path);
					chunkservers.get(CS).setLastGoodTime(System.currentTimeMillis());
				}
				catch(RemoteException re){
					System.out.println("Error connecting to chunkserver " + CS);
					chunkservers.get(CS).setStatus(CSStatus.DOWN);
				}
			}
			try{
				clients.get(clientID).requestStatus("deleteFile", path, true, -1);
			}
			catch(RemoteException re){
				System.out.println("Error connecting to client.");
			}
		}
		else
		{
			System.out.println("Delete unsuccessful. " + path + " does not exist.");
			try{
				clients.get(clientID).requestStatus("deleteDirectory", path, false, -1);
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
		Node file = directory.root.find(directory.pathTokenizer(chunkhandle), 1);
		if(file == null)
		{
			try{
				System.out.println("Cannot append to a null file " + chunkhandle);
				clients.get(clientID).requestStatus("append", chunkhandle, false, -1);
			}
			catch(RemoteException re){
				System.out.println("Error connecting to client.");
			}
		}
		else{
			try{
				clients.get(clientID).passMetaData(chunkhandle, -1, file.chunkServersNum, reqID);
				System.out.println("Metadata sent to client.");
			}
			catch(RemoteException re){
				System.out.println("Error connecting to client.");
			}
			log.makeLogRecord(System.currentTimeMillis(), chunkhandle, "append", 1);
			Storage.storeLog(log);
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
			System.out.println("Cannot find metadata. " + chunkhandle + " does not exist.");
			try{
				clients.get(clientID).requestStatus("read", chunkhandle, false, -1);
			}
			catch(RemoteException re){
				System.out.println("Error connecting to client.");
			}
		}
		else{
			try{
				clients.get(clientID).passMetaData(chunkhandle, -1, file.chunkServersNum, reqID);
				System.out.println("Metadata sent to client.");
			}
			catch(RemoteException re){
				System.out.println("Error connecting to client.");
			}
			log.makeLogRecord(System.currentTimeMillis(), chunkhandle, "read", 1);
			Storage.storeLog(log);
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
				System.out.println("Cannot atomic append. " + chunkhandle + " does not exist.");
				clients.get(clientID).requestStatus("atomicAppend", chunkhandle, false, -1);
			}
			catch(RemoteException re){
				System.out.println("Error connecting to client.");
			}
		}
		else if (file.getPrimaryLeaseTime() > (System.currentTimeMillis() - MINUTE))
		{
			try{
				System.out.println("Primary lease exists for " + file.getPrimaryChunkserver());
				clients.get(clientID).passMetaData(chunkhandle, file.getPrimaryChunkserver(), file.chunkServersNum, reqID);
			}
			catch(RemoteException re){
				System.out.println("Error connecting to client.");
			}
			log.makeLogRecord(System.currentTimeMillis(), chunkhandle, "atomicAppend", 1);
			Storage.storeLog(log);
		}
		else
		{
			int randomCS = getRandomWorkingCS(file.chunkServersNum);
			if (randomCS == -1){
				try{
					System.out.println("Unable to issue primary lease.");
					clients.get(clientID).requestStatus("atomicAppend", chunkhandle, false, -1);
				}
				catch(RemoteException re){
					System.out.println("Error connecting to client.");
				}
			}
			file.issuePrimaryLease(randomCS, System.currentTimeMillis());
			boolean primaryLeaseSuccess;
			ArrayList<Integer> secondaries = new ArrayList<Integer>();
			for(Integer CS: file.chunkServersNum){
				if(CS != randomCS){
					secondaries.add(new Integer(CS));
				}
			}
			try{
				System.out.println("Issued primary lease to " + file.getPrimaryChunkserver());
				chunkservers.get(randomCS).getCS().primaryLease(chunkhandle, secondaries);
			}
			catch(RemoteException re){
				System.out.println("Error connecting to chunkserver " + 1);
				chunkservers.get(1).setStatus(CSStatus.DOWN);
				primaryLeaseSuccess = false;
			}
			primaryLeaseSuccess = true;
			if(primaryLeaseSuccess){
				try{
					System.out.println("Passing metadata for atomic append to client.");
					clients.get(clientID).passMetaData(chunkhandle, file.getPrimaryChunkserver(), file.chunkServersNum, reqID);
				}
				catch(RemoteException re){
					System.out.println("Error connecting to client.");
				}
				log.makeLogRecord(System.currentTimeMillis(), chunkhandle, "atomicAppend", 1);
				Storage.storeLog(log);
			}
			else{
				try{
					System.out.println("Unable to issue primary lease.");
					clients.get(clientID).requestStatus("atomicAppend", chunkhandle, false, -1);
				}
				catch(RemoteException re){
					System.out.println("Error connecting to client.");
				}				
			}
		}	
	}

	/**
	 * Restore chunkserver is passed the ID for the chunkserver that has gone down and is now recovering.
	 * @param CSID
	 * @throws RemoteException
	 */
	public void restoreChunkserver(int CSID) throws RemoteException {
		System.out.println("Beginning restoration of " + CSID);
		System.out.println("log length " + log.getLength());
		if(!clients.isEmpty()) {
			/* Iterate through clients detected by the master.
			 * Connect Chunkserver(CSID) to all clients.
			 * Connect all clients to Chunkserver(CSID)
			*/
			for(Map.Entry<Integer, ClientInterface> entry : this.clients.entrySet()) {
				try {
					chunkservers.get(CSID).getCS().connectToClient(entry.getKey());
					entry.getValue().connectToChunkserver(CSID);
					System.out.println("Chunkserver " + CSID + " connected to Client " + entry.getKey());
				} catch (RemoteException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		/*
		 * If log length is 0, no transactions have been sent to the master.
		 */
		if(log.getLength() == 0){
			chunkservers.get(CSID).setLastGoodTime(System.currentTimeMillis());
			chunkservers.get(CSID).setStatus(CSStatus.OK);
			System.out.println("Recovery complete for " + CSID);
			return;
		}
		long lastGoodTime = chunkservers.get(CSID).getLastGoodTime();
		
		String logRecord = log.getReference(0);
		int count = 0;
		String[] fields = logRecord.split("\\$");
		for(int i = 0; i < fields.length; i++) {
			System.out.println(i + ": " + fields[i]);
		}
		Node file = directory.root.find(directory.pathTokenizer(fields[2]), 1);

		long logTime = Long.parseLong(fields[1]);
		//for now, this scans through the entire transaction log
		//it assumes that the transaction log has entries since the last good time
		while (lastGoodTime > logTime && count < log.getLength()){
			System.out.println("no need to redo this transaction");			
			System.out.println("count: " + count);
			logRecord = log.getReference(count);
			fields = logRecord.split("\\$");
			logTime = Long.parseLong(fields[1]);
			count++;

		}
		while (count < log.getLength()){
			System.out.println("in main while loop");
			System.out.println("count in main while: " + count);
			logRecord = log.getReference(count);
			System.out.println(log.getReference(count));
			fields = logRecord.split("\\$");
			file = directory.root.find(directory.pathTokenizer(fields[2]), 1);
			System.out.println("file: " + file);
			if(fields[4].equals("0") || (file == null && !fields[3].contains("delete"))
					|| (file != null && !file.chunkServersNum.contains(CSID))){
				System.out.println("field 4 = 0; ignoring; continue, count = " + count);
				count++;
				continue;
			}
			switch(fields[3]){
			case "append":
				if(!fetchAndRewrite(fields[2], CSID)){
					chunkservers.get(CSID).setStatus(CSStatus.DOWN);
					return;					
				}
				chunkservers.get(CSID).setLastGoodTime(Long.parseLong(fields[1]));
				break;
			case "createFile":
				if(!createFileRedo(fields[2], CSID)){
					chunkservers.get(CSID).setStatus(CSStatus.DOWN);
					return;
				}
				chunkservers.get(CSID).setLastGoodTime(Long.parseLong(fields[1]));
				break;
			case "createDirectory":
				if(!createDirectoryRedo(fields[2], CSID)){
					chunkservers.get(CSID).setStatus(CSStatus.DOWN);
					return;
				}
				chunkservers.get(CSID).setLastGoodTime(Long.parseLong(fields[1]));
				break;
			case "atomicAppend":
				if(!fetchAndRewrite(fields[2], CSID)){
					chunkservers.get(CSID).setStatus(CSStatus.DOWN);
					return;
				}
				
				chunkservers.get(CSID).setLastGoodTime(Long.parseLong(fields[1]));
				break;
			case "deleteFile":
				if(!deleteFileRedo(fields[2], CSID)){
					System.out.println("trying to delete file.");
					chunkservers.get(CSID).setStatus(CSStatus.DOWN);
					return;
				}
				chunkservers.get(CSID).setLastGoodTime(Long.parseLong(fields[1]));
				break;
			case "deleteDirectory":
				if(!deleteDirectoryRedo(fields[2], CSID)){
					chunkservers.get(CSID).setStatus(CSStatus.DOWN);
					return;
				}
				chunkservers.get(CSID).setLastGoodTime(Long.parseLong(fields[1]));
				break;	
			default:
				System.out.println("Error parsing log file.");
				break;
			}
			count++;
			System.out.println("count: " + count);
		}
		chunkservers.get(CSID).setLastGoodTime(System.currentTimeMillis());
		chunkservers.get(CSID).setStatus(CSStatus.OK);
		System.out.println("Recovery complete for " + CSID);
	}

	/**
	 * The Chunkserver Restoration algorithm will call this method if a chunkserver needs to run
	 * a createDirectory to catch up with current version of data.
	 * @param path
	 * @param chunkserverID
	 * @return
	 * @throws RemoteException
	 */
	public boolean createDirectoryRedo(String path, int chunkserverID) throws RemoteException {
		try{
			chunkservers.get(chunkserverID).getCS().createDirectory(path);
			chunkservers.get(chunkserverID).setLastGoodTime(System.currentTimeMillis());
		}
		catch (RemoteException re){
			System.out.println("Error connecting to chunkserver " + chunkserverID);
			chunkservers.get(chunkserverID).setStatus(CSStatus.DOWN);
			return false;
		}
		return true;
	}

	/**
	 * The Chunkserver Restoration algorithm will call this method if a chunkserver needs to
	 * create a file it should have.
	 * @param chunkhandle
	 * @param chunkserverID
	 * @return
	 * @throws RemoteException
	 */
	public boolean createFileRedo(String chunkhandle, int chunkserverID) throws RemoteException {
		try{
			chunkservers.get(chunkserverID).getCS().createFile(chunkhandle);
			chunkservers.get(chunkserverID).setLastGoodTime(System.currentTimeMillis());
		}
		catch (RemoteException re){
			System.out.println("Error connecting to chunkserver " + chunkserverID);
			chunkservers.get(chunkserverID).setStatus(CSStatus.DOWN);
			return false;
		}
		return true;
	}

	/**
	 * The Chunkserver Restoration algorithm will call this method if the recovering chunkserver has a
	 * file that, according to the Master, should not exist in the namespace.
	 * @param chunkhandle
	 * @param chunkserverID
	 * @return
	 * @throws RemoteException
	 */
	public boolean deleteFileRedo(String chunkhandle, int chunkserverID) throws RemoteException {
		try{
			chunkservers.get(chunkserverID).getCS().deleteFile(chunkhandle);
			chunkservers.get(chunkserverID).setLastGoodTime(System.currentTimeMillis());
		}
		catch (RemoteException re){
			System.out.println("Error connecting to chunkserver " + chunkserverID);
			chunkservers.get(chunkserverID).setStatus(CSStatus.DOWN);
			return false;
		}
		return true;
	}

	/**
	 * The Chunkserver Restoration algorithm will call this method if the recovering chunkserver has a
	 * directory that, according to the Master, should not exist in the namespace.
	 * @param path
	 * @param chunkserverID
	 * @return
	 * @throws RemoteException
	 */
	public boolean deleteDirectoryRedo(String path, int chunkserverID) throws RemoteException {
		try{
			chunkservers.get(chunkserverID).getCS().deleteDirectory(path);
			chunkservers.get(chunkserverID).setLastGoodTime(System.currentTimeMillis());
		}
		catch (RemoteException re){
			System.out.println("Error connecting to chunkserver " + chunkserverID);
			chunkservers.get(chunkserverID).setStatus(CSStatus.DOWN);
			return false;
		}
		return true;
	}

	/**
	 * The Chunkserver Restoration algorithm will call this method if the recovering chunkserver is
	 * behind on an append request.  The system will look for replicas that have the file with up-to-date
	 * timestamps.
	 * @param chunkhandle
	 * @param chunkserverID
	 * @return
	 * @throws RemoteException
	 */
	public boolean fetchAndRewrite(String chunkhandle, int chunkserverID) throws RemoteException {
		Node file = directory.root.find(directory.pathTokenizer(chunkhandle), 1);
		int source = file.chunkServersNum.get(0);
		int count = 0;
		while(chunkservers.get(source).getStatus() != CSStatus.OK){
			count++;
			source = file.chunkServersNum.get(count);
			if(count == file.chunkServersNum.size()){
				System.out.println("No remaining healthy replicas. Skipping file " + chunkhandle);
				return true;
			}
		}
		try{
			chunkservers.get(chunkserverID).getCS().fetchAndRewrite(chunkhandle, source);
			chunkservers.get(chunkserverID).setLastGoodTime(System.currentTimeMillis());
		}
		catch(RemoteException re){
			System.out.println("Error connecting to chunkserver " + chunkserverID);
			chunkservers.get(chunkserverID).setStatus(CSStatus.DOWN);
			return false;
		}
		return true;
	}

	/**
	 * Returns a random chunkserver index when passed a list of Chunkservers.
	 * @param chunkServersNum
	 * @return
	 */
	public int getRandomWorkingCS (List<Integer> chunkServersNum){
		Random randInt = new Random();
		int chosenIndex = Math.abs(randInt.nextInt() % chunkServersNum.size());
		int chosenID = chunkServersNum.get(chosenIndex);
		boolean working = false;
		int attempts = 0;
		if(chunkservers.get(chosenID).getStatus() == CSStatus.OK){
			working = true;
		}
		else{
			working = false;
		}
		while(!working){
			chosenIndex = Math.abs(randInt.nextInt() % chunkServersNum.size());
			chosenID = chunkServersNum.get(chosenIndex);
			attempts++;
			if(chunkservers.get(chosenID).getStatus() == CSStatus.OK){
				working = true;
			}
			else{
				working = false;
			}
			if(attempts > 10){
				System.out.println("Unable to locate a working chunkserver.");
				return -1;
			}
		}
		return chosenID;
	}

	public static void main(String[] args) {
		try {
			Master master = new Master();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			System.out.println("Error in creating Master instance");
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
					System.out.println("Unexpected exception caught in Master thread:" + e);
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
		long startTime;

		// called by createFile
		public Task(TaskType type, String path, String fileName, int numReplicas, int clientID){
			this.type = type;
			this.path = path;
			this.fileName = fileName;
			this.numReplicas = numReplicas;
			this.clientID = clientID;
			startTime = System.currentTimeMillis();
		}

		// called by read, append, and atomicAppend 
		public Task(TaskType type, String path, int clientID, int reqID){
			this.type = type;
			this.path = path;
			this.clientID = clientID;
			this.reqID = reqID;
			startTime = System.currentTimeMillis();
		}

		// called by deleteFile, createDirectory, deleteDirectory
		public Task(TaskType type, String path, int clientID){
			this.type = type;
			this.path = path;
			this.clientID = clientID;
			this.reqID = -1;
			startTime = System.currentTimeMillis();
		}

		// called by heartbeat
		public Task(TaskType type, int CSID){
			this.type = type;
			this.CSID = CSID;
			startTime = System.currentTimeMillis();
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
		public long getStartTime(){
			return startTime;
		}
	}


	/**
	 * Class CSInfo is used by master to maintain metadata for connecting to various
	 * chunkservers. It keeps track of the most recent heartbeat time received from
	 * the chunkserver, the status of the chunkserver, and the chunkserver ID.
	 * @author lazzarid
	 *
	 */
	public enum CSStatus {DOWN, OK, RECOVERING};
	private class CSInfo{
		CSStatus status;
		long lastHeartbeat;
		long lastGoodTime;
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

		public long getLastGoodTime(){
			return lastGoodTime;
		}

		public long getLastHB(){
			return lastHeartbeat;
		}

		public ChunkserverInterface getCS(){
			return remoteCS;
		}

		public void setCS(ChunkserverInterface ci) {
			this.remoteCS = ci;
		}
		public int getID(){
			return id;
		}

		public void setLastGoodTime(long lastGoodTime){
			this.lastGoodTime = lastGoodTime;
		}

		public void setStatus(CSStatus status){
			this.status = status;
		}

		public void setLastHB(long LHB){
			this.lastHeartbeat = LHB;
		}
	}

	/**
	 * Heartbeat class runs on its own Thread, and periodically messages chunkservers to determine
	 * their status.
	 */
	private class Heartbeat extends Thread{
		long lastHBTime;

		public Heartbeat(){
			lastHBTime = 0;
		}

		public void run(){
			while(true){
				// send heartbeat message to chunkservers
				for(Integer i:chunkservers.keySet()){
					boolean downCS = false;
					try {
						chunkservers.get(i).getCS().heartbeat();
						System.out.println("Master sent heartbeat to " + i);
					} catch (RemoteException e) {
						downCS = true;
						System.out.println("Could not connect to chunkserver " + i + " for heartbeat");
					}
					if (downCS && chunkservers.get(i).getStatus() != CSStatus.DOWN){
						//chunkserver has gone down
						System.out.println("down " + i);
						chunkservers.get(i).setStatus(CSStatus.DOWN);
					}
					else if (!downCS && chunkservers.get(i).getStatus() == CSStatus.DOWN){
						System.out.println("need to recover" + i);
						//initiate chunkserver recovery
						tasks.add(new Task(TaskType.recoverCS, i));
						chunkservers.get(i).setStatus(CSStatus.RECOVERING);
						stateChanged();
					}
					else if (!downCS && chunkservers.get(i).getStatus() == CSStatus.OK){
						System.out.println("cs okay " + i);
						//chunkserver is still ok, so we assume up to date
						chunkservers.get(i).setLastHB(System.currentTimeMillis());
						chunkservers.get(i).setLastGoodTime(System.currentTimeMillis());
					}
					else if (!downCS && chunkservers.get(i).getStatus() == CSStatus.RECOVERING){
						System.out.println("still recovering " + i);
						//chunkserver is recovering so we do not assume this is a good time
						chunkservers.get(i).setLastHB(System.currentTimeMillis());
					}
				}
				lastHBTime = System.currentTimeMillis();
				// gap time between sends
				try {
					sleep(HEARTBEAT_DELAY);
				} catch (InterruptedException e) {
					System.err.println("InterruptedException in master heartbeat");
				}
			}
		}
	}
}

