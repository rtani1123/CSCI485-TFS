package Client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.rmi.Naming;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Semaphore;

import Interfaces.ChunkserverInterface;
import Interfaces.ClientInterface;
import Interfaces.MasterInterface;

/**
 * The Client class takes a request from the Application.  It then contacts the
 * Master and/or Chunkservers appropriately based on the request.  For appends,
 * atomic appends, or reads it creates a Request object to handle the data
 * intermediately.  For read requests, it may use a stored ClientMetaDataItem in
 * place of contacting the Master to save time. This MetadataItem will expire after
 * a set period of time, in which case the Client will have to contact the Master
 * to get the most current metadata.
 */
public class Client extends UnicastRemoteObject implements ClientInterface {
	ArrayList<ClientMetaDataItem> clientMetaDataArray; // locations of replicas
	// and primary lease
	Map<Integer, ChunkserverInterface> chunkservers;
	//	List<ChunkserverInterface> chunkservers; // chunkservers to contact
	List<Request> pendingRequests; // application request info for append,
	// atomic append, and read
	int clientID; // ID of this client
	MasterInterface master; // master to contact
	int count; // used to create unique request IDs
	Semaphore countLock = new Semaphore(1, true); // semaphore for creating
	MasterState masterState = new MasterState(); //class for checking existence of master.
	// unique request IDs

	// requestType strings
	public static final String APPEND = "append";
	public static final String ATOMIC_APPEND = "atomicAppend";
	public static final String READ = "read";
	public static final String READ_COMPLETELY = "readCompletely";
	public static final String NUM_FILES = "numFiles";

	public static final long METADATA_LIFESPAN = 10000;
	//	public static Client myClient=null;
	public static void main(String[] args) throws RemoteException {

	}

	// constructor, takes an ID for the client
	public Client(int ID) throws RemoteException {
		clientMetaDataArray = new ArrayList<ClientMetaDataItem>();
		pendingRequests = Collections
				.synchronizedList(new ArrayList<Request>());
		clientID = ID;
		count = 0;
		chunkservers = Collections
				.synchronizedMap(new HashMap<Integer, ChunkserverInterface>());

		setupClientHost(clientID);
		masterState.start();
	}

	/**
	 * URL Protocol to establish RMI Server on Client-side.
	 */
	public void setupClientHost(Integer clientID) throws RemoteException {
		try {
			System.setSecurityManager(new RMISecurityManager());
			Registry registry = LocateRegistry.createRegistry(clientID);

			Naming.rebind("rmi://dblab-43.vlab.usc.edu:"+clientID+"/CLIENT" + clientID, this);

			System.out.println("Client Host Setup Success");
		} catch (MalformedURLException re) {
			System.out.println("Bad connection - MalformedURLException");
		} catch (RemoteException e) {
			System.out.println("Bad connection - RemoteException");
		} catch (Exception e) {		}
		// now run Master.
	}

	/**
	 * Connecting Client(clientID) to Master, RMI.
	 */
	public void connectToMaster() throws RemoteException {
		try {
			System.setSecurityManager(new RMISecurityManager());
			/*
			 * format for connection should be "rmi:DOMAIN/ChunkserverID".
			 * ChunkserverID will be different for each instance of Chunkserver
			 * one detail to mention is that CSMaster will not be the same as
			 * MasterCS. There's actually a completely different callfor master
			 * calling CS functions than CS calling master functions.
			 * 
			 * For this, the master is hosted on dblab-29.
			 */
			master = (MasterInterface) Naming
					.lookup("rmi://dblab-18.vlab.usc.edu/MASTER");
			System.out.println("Connection to Master Success");
			/*
			 * ChunkServer FUNCTION HOST implementation
			 */

		} catch (Exception re) {
			System.out.println("Failure to connect to Master");
		}
	}

	/**
	 * When the client connects, it requests the most current list of chunkservers from the Master.
	 * The client then automatically attempts to connect to each chunkserver in a reciprocal RMI instance.
	 * @param CS
	 */
	public void setChunkservers(Map<Integer, ChunkserverInterface> CS) {
		for(Map.Entry<Integer, ChunkserverInterface> entry : CS.entrySet()) {
			chunkservers.put(entry.getKey(), entry.getValue());
			connectToChunkserver(entry.getKey());
			System.out.println("Connection to " + entry.getKey() + " successful.");
			try {
				entry.getValue().connectToClient(clientID);
				System.out.println("Chunkserver " + entry.getKey() + " connected to Client " + clientID);
			} catch (RemoteException e) {
				System.out.println("Client connection to Chunkserver " + entry.getKey() + " failed");
			}
		}
	}

	/**
	 * Connection to Chunkserver method.
	 * @param id
	 */
	public void connectToChunkserver(Integer id) {
		try {
			System.setSecurityManager(new RMISecurityManager());
			ChunkserverInterface tempCS = null;
			//connect based on unique Chunkserver ID
			if(id == 1)
				tempCS = (ChunkserverInterface)Naming.lookup("rmi://dblab-36.vlab.usc.edu:123/CHUNK" + id.toString());
			else if(id == 2)
				tempCS = (ChunkserverInterface)Naming.lookup("rmi://dblab-05.vlab.usc.edu:124/CHUNK" + id.toString());
			else if(id == 3)
				tempCS = (ChunkserverInterface)Naming.lookup("rmi://dblab-29.vlab.usc.edu:125/CHUNK" + id.toString());

		
			chunkservers.put(id,tempCS);
			/*
			 * ChunkServer FUNCTION HOST implementation
			 */
			System.out.println("Connection to Chunkserver " + id
					+ " Success");

		} catch (Exception re) {
			System.out.println("Failure to connect to Chunkserver " + id);
		}
	}


	/**
	 * The application requests a file to be created on numReplicas chunkservers.
	 * Request passed to Master.
	 * @param path
	 * @param fileName
	 * @param numReplicas
	 */
	// called by the application
	public void createFile(String path, String fileName, int numReplicas)
			throws RemoteException {
		try {
			master.createFile(path, fileName, numReplicas, clientID);
		} catch (RemoteException e) {
			System.out.println("Could not connect to master to create file.");
			masterState.stateChanged();
		}
	}

	/**
	 * The application requests a directory to be created.
	 * Created on all chunkservers.
	 * Request passed to Master.
	 * @param path
	 */
	// called by the application
	public void createDirectory(String path) throws RemoteException {
		try {
			master.createDirectory(path, clientID);
		}
		catch(RemoteException e){
			System.out.println("Could not connect to master to create directory.");
			masterState.stateChanged();
		}
	}

	/**
	 * The application requests a file to be deleted from the namespace and all instances of it in chunkservers.
	 * Request passed to Master.
	 * @param chunkhandle
	 */
	// called by the application
	public void deleteFileMaster(String chunkhandle) throws RemoteException {
		try {
			master.deleteFileMaster(chunkhandle, clientID);
		} catch (RemoteException e) {
			System.out.println("Could not connect to master to delete file.");
			masterState.stateChanged();
		}
	}

	/**
	 * The application requests a directory be deleted from the namespace and all instances of it in chunkservers.
	 * Request passed to Master.
	 * @param path
	 */
	// called by the application
	public void deleteDirectory(String path) throws RemoteException {
		try {
			master.deleteDirectory(path, clientID);
		} catch (RemoteException e) {

			System.out.println("Could not connect to master to delete directory.");
			masterState.stateChanged();
		}
	}

	/**
	 * Append request from Application.
	 * User specifies destination, offset, length of data and byte array which is derived
	 * from a source file on user's local machine which is converted to bytes.
	 * 
	 * Request passed to Master.
	 * 
	 * @param chunkhandle
	 * @param offset
	 * @param length
	 * @param data
	 * @param withSize
	 */
	// called by the application. For Appends, the request is stored in an internal Client Array
	public void append(String chunkhandle, int offset, int length, byte[] data,
			boolean withSize) throws RemoteException { // if no metadata is
		// stored on the
		// chunkhandle, ask
		// master for location
		int id = -1;
		System.out.println(chunkhandle + " " + offset + " " + length + " "+data + " " +withSize);
		try {
			countLock.acquire();
			id = ++count;
			//instantiate a new request with all the append information and store it in the array of pendingRequests.
			Request r = new Request(APPEND, chunkhandle, count);
			r.setData(data); //set data to append
			r.setLength(length); //set length of data
			r.setOffset(offset); //set offset where data is to be appeneded
			r.setWithSize(withSize); //set boolean whether size information is being passed
			pendingRequests.add(r);
			master.append(chunkhandle, clientID, count);
			countLock.release();
		} catch (RemoteException e) {
			System.out.println("Could not connect to master to append.");
			masterState.stateChanged();
			// Remove request from the list of pending requests if could not connect to master
			int index = -1;
			for (int x = 0; x < pendingRequests.size(); x++) {
				if ((pendingRequests.get(x)).getReqID() == id) {
					index = x;
					break;
				}
			}
			pendingRequests.remove(index);

		} catch (InterruptedException e) {
			System.out.println("Interrupted Exception in append method");
		}
	}

	/**
	 * Atomic append request from Application.
	 * User specifies destination, length of data and byte array which is derived
	 * from a source file on user's local machine which is converted to bytes.
	 * 
	 * Request passed to Master.
	 * 
	 * @param chunkhandle
	 * @param length
	 * @param data
	 * @param withSize
	 */

	// called by the application. For Atomic Appends, the request is stored in an internal Client Array
	public void atomicAppend(String chunkhandle, int length, byte[] data, boolean withSize) throws RemoteException {
		int id = -1;
		try {
			countLock.acquire();
			id = ++count; //set a unique ID for the request
			Request r = new Request(ATOMIC_APPEND, chunkhandle, count);
			r.setData(data); //set data to be appended atomically
			r.setLength(length); //set length of data
			r.setWithSize(withSize); //set boolean determining whether size is given
			pendingRequests.add(r);
			master.atomicAppend(chunkhandle, clientID, count);
			countLock.release();
		} catch (RemoteException e) {
			System.out.println("Could not connect to master to atomic append.");
			masterState.stateChanged();
			// Remove request if could not connect to master
			int index = -1;
			for (int x = 0; x < pendingRequests.size(); x++) {
				if ((pendingRequests.get(x)).getReqID() == id) {
					index = x;
					break;
				}
			}
			pendingRequests.remove(index);
		} catch (InterruptedException e) {
			System.out.println("Interrupted Exception in atomic append method");
		}
	}

	/**
	 * Application calls read file.  File is pulled from a chunkserver to the client, written
	 * to a new file on Client local machine, specified by Application.
	 * 
	 * @param chunkhandle
	 * @param offset
	 * @param length
	 * @param destination
	 * 
	 */
	// called by the application. For Read, the request is stored in an internal Client Array. The client first checks its own MetaData to see if it already
	//has valid information on the chunkhandle, otherwise it contacts master.
	public void read(String chunkhandle, int offset, int length, String destination)
			throws RemoteException {
		int index = alreadyInClientMetaData(chunkhandle); // method returns index of item if the chunkhandlealready exists,otherwise it returns -1;
		// metadata exists and has not expired; do not contact master
		if (index > -1 && (clientMetaDataArray.get(index).getTimestamp() + METADATA_LIFESPAN) > System.currentTimeMillis()) { 
			try {
				countLock.acquire();
				count++;
				// this constructor adds the request knowing that it already has
				// the server locations
				ClientMetaDataItem i = (ClientMetaDataItem) clientMetaDataArray
						.get(index);
				Request r = new Request(READ, chunkhandle, count,i.chunkservers);
				r.setLength(length);
				r.setOffset(offset);
				r.setDestination(destination);
				pendingRequests.add(r);
				countLock.release();
				contactChunks(r.getReqID());
			} catch (InterruptedException e) {
				System.out.println("Interrupted Exception in read method");
			}
		} 
		// metadata does not exist or has expired
		else {
			// if metadata exists but has expired, remove it.
			if(index > -1){
				clientMetaDataArray.remove(index);
			}
			int id = -1;
			//create a new request that needs to be sent to master and add it to the list of pending requests.
			try {
				countLock.acquire();
				id = ++count;
				Request temp = new Request(READ, chunkhandle, count);
				temp.setDestination(destination);
				temp.setLength(length);
				temp.setOffset(offset);
				pendingRequests.add(temp);
				master.read(chunkhandle, clientID, count);
				countLock.release();
			} catch (RemoteException e) {
				System.out.println("Could not connect to master to read.");
				masterState.stateChanged();
				// Remove request if could not connect to master
				int ind = -1;
				for (int x = 0; x < pendingRequests.size(); x++) {
					if (pendingRequests.get(x).getReqID() == id) {
						ind = x;
						break;
					}
				}
				pendingRequests.remove(ind);
			} catch (InterruptedException e) {
				System.out.println("Interrupted Exception in read method");
			}
		}
	}
	
	//Function to get the number of files appended (Test 7)
	public void numFiles(String chunkhandle){
		int index = alreadyInClientMetaData(chunkhandle); // method returns index of item if the chunkhandle already exists, otherwise it returns -1;
		// metadata exists and has not expired; do not contact master
		if (index > -1 && (clientMetaDataArray.get(index).getTimestamp() + METADATA_LIFESPAN) > System.currentTimeMillis()) { 
			try {
				countLock.acquire();
				count++;
				// this constructor adds the request knowing that it already has
				// the server locations
				ClientMetaDataItem i = (ClientMetaDataItem) clientMetaDataArray.get(index);
				Request r = new Request(NUM_FILES, chunkhandle, count,i.chunkservers);
				pendingRequests.add(r);
				countLock.release();
				contactChunks(r.getReqID());
			} catch (InterruptedException e) {
				System.out.println("Interrupted Exception in read completely method");
			}
		} 
		// metadata does not exist or has expired
		else{
			// metadata exists but has expired
			if(index > -1){
				clientMetaDataArray.remove(index);
			}
			int id = -1;
			try { //create a new request to master and add it to the list of pending requests.
				countLock.acquire();
				id = ++count;
				Request temp = new Request(NUM_FILES, chunkhandle, count);
				pendingRequests.add(temp);
				master.read(chunkhandle, clientID, count);
				countLock.release();
			} catch (RemoteException e) {
				System.out.println("Could not connect to master to read completely.");
				masterState.stateChanged();
				// Remove request if could not connect to master
				int ind = -1;
				for (int x = 0; x < pendingRequests.size(); x++) {
					if (pendingRequests.get(x).getReqID() == id) {
						ind = x;
						break;
					}
				}
				pendingRequests.remove(ind);
			} catch (InterruptedException e) {
				System.out.println("Interrupted Exception in read completely method");
			}
		}
	}
	/**
	 * Application calls read file completely.  File is pulled from a chunkserver to the client, written
	 * to a new file on Client local machine, specified by Application.
	 * 
	 * @param chunkhandle
	 * @param destination
	 * 
	 */
	public void readCompletely(String chunkhandle, String destination) throws RemoteException {
		int index = alreadyInClientMetaData(chunkhandle); // method returns index of item if the chunkhandle already exists, otherwise it returns -1;
		// metadata exists and has not expired; do not contact master
		if (index > -1 && (clientMetaDataArray.get(index).getTimestamp() + METADATA_LIFESPAN) > System.currentTimeMillis()) { 
			try {
				countLock.acquire();
				count++;
				// this constructor adds the request knowing that it already has
				// the server locations
				ClientMetaDataItem i = (ClientMetaDataItem) clientMetaDataArray.get(index);
				Request r = new Request(READ_COMPLETELY, chunkhandle, count,i.chunkservers);
				r.setDestination(destination);
				pendingRequests.add(r);
				countLock.release();
				contactChunks(r.getReqID());
			} catch (InterruptedException e) {
				System.out.println("Interrupted Exception in read completely method");
			}
		} 
		// metadata does not exist or has expired
		else{
			// metadata exists but has expired
			if(index > -1){
				clientMetaDataArray.remove(index);
			}
			int id = -1;
			try {
				countLock.acquire();
				id = ++count;
				Request temp = new Request(READ_COMPLETELY, chunkhandle, count);
				temp.setDestination(destination);
				pendingRequests.add(temp);
				master.read(chunkhandle, clientID, count);
				countLock.release();
			} catch (RemoteException e) {
				System.out.println("Could not connect to master to read completely.");
				masterState.stateChanged();
				// Remove request if could not connect to master
				int ind = -1;
				for (int x = 0; x < pendingRequests.size(); x++) {
					if (pendingRequests.get(x).getReqID() == id) {
						ind = x;
						break;
					}
				}
				pendingRequests.remove(ind);
			} catch (InterruptedException e) {
				System.out.println("Interrupted Exception in read completely method");
			}
		}
	}

	/**
	 * Client status request called from Chunkserver and Master.
	 * 
	 * @param requestType
	 * @param fullPath
	 * @param succeeded
	 * @param ID
	 */
	// called by master
	public void requestStatus(String requestType, String fullPath,
			boolean succeeded, int ID) throws RemoteException {
		if (succeeded) {
			System.out.println("Request to" + requestType + " " + fullPath
					+ "succeeded.");
		} else {
			System.out.println("Request to" + requestType + " " + fullPath
					+ "failed.");
		}
	}

	/**
	 * Master calls passMetaData which gives Client metadata, parameters of which are noted.
	 * This method references a request ID which has already been processed by the Client in another method
	 * (append, atomic append, etc).  Finishes method with a call to contactChunks, passing the request id
	 * for the request to be serviced.
	 * 
	 * @param chunkhandle
	 * @param pID
	 * @param chunkserversList
	 * @param reqID
	 */
	// Method called by master giving chunkhandle and chunkservers
	public void passMetaData(String chunkhandle, int pID, List<Integer> chunkserversList, int reqID) {
		try{
			System.err.println(chunkhandle + " " + pID+ " " +reqID+ " " +chunkserversList);
			// reqID of -1 is used for functions such as Creates and Deletes which are not stored in pendingRequests
			if (reqID != -1) {
				// Go through the pendingRequests array to find request with the matching reqID
				// Save locations of replicates and/or update primary lease
				for (int i = 0; i < pendingRequests.size(); i++) {
					Request r = pendingRequests.get(i);
					// if the reqID's are matching
					if (reqID == r.getReqID()) {
						r.setCS(chunkserversList);
						r.setReceived();
						boolean exists = false; // boolean to check if this
						// chunkhandle already exists in the client's metadata
						for (int j = 0; j < clientMetaDataArray.size(); j++) {
							// if the chunkhandle is found, exit the loop
							if ((clientMetaDataArray.get(j).chunkhandle).equals(chunkhandle)) {
								exists = true;
								(clientMetaDataArray.get(j)).setID(pID); 
								(clientMetaDataArray.get(j)).setChunkservers(chunkserversList);
								// update the primary lease in case it is different/has changed
								break;
							}
						}
						// if the chunkhandle was not already in the metadata, add it along with its chunkservers
						if (!exists) {
							clientMetaDataArray.add(new ClientMetaDataItem(chunkhandle, pID, chunkserversList, System.currentTimeMillis()));
						}
						// once the corresponding ReqID is found, break out of the outer loop
						break;
					}
				}
				for(int i = 0; i < pendingRequests.size(); i++){
					if(pendingRequests.get(i).getFullPath().equals(chunkhandle)){
						pendingRequests.get(i).setPrimaryID(pID);
					}
				}
				contactChunks(reqID);
			}
		}catch(Exception e){
			System.out.println("PassMetaData error in Client");
			e.printStackTrace();
		}
	}
	/**
	 * 
	 * @param chunkhandle
	 * @return Returns -1 if the passed chunkhandle does not exist in the current client metadata.
	 * Else, it returns the value of the client metadata index that contains the passed chunkhandle.
	 */

	// return index of metadata for chunkhandle if already known
	// otherwise return -1
	private int alreadyInClientMetaData(String chunkhandle) {
		for (int i = 0; i < clientMetaDataArray.size(); i++) {
			if (chunkhandle.equals(clientMetaDataArray.get(i).chunkhandle))
				return i;
		}
		return -1;
	}

	/**
	 * Sends request to chunkservers for either Append, Atomic Append or Read requests.
	 * @param rID
	 */
	// call this to contact chunkservers from available list
	private void contactChunks(int rID) {
		System.out.println("contacting chunks... ");
		//for loop to parse through requests
		for (int i = 0; i < pendingRequests.size(); i++) {
			Request r = (Request) pendingRequests.get(i);
			//if no chunkservers are available exit the loop
			if(r.getChunkservers().size()==0){
				System.out.println("There is no available chunkserver to contact with, please try again in a few mintues.");
				break;
			}
			//if the correct request is found, display them to the console and perform the request	
			if (r.getReqID() == rID) {
				//if the request is an APPEND, perform the append.
				if ((r.getRequestType()).equals(APPEND)) {
					System.out.println("chunkservers for append in contactCS func: "+r.getChunkservers());
					//parse through the available chunkservers and append
					for (int cs : r.getChunkservers()) {
						System.out.println(cs);
						try {
							if (chunkservers.get(cs).append(r.getFullPath(), r.getPayload(), r.getLength(), r.getOffset(), r.getWithSize())) {
								System.out.println("Successful append");
							} else {
								System.out.println("Failed append");
							}
						} catch (RemoteException e) {
							System.out.println("Failed to connect to chunkserver " +cs +" for append ");
						}
					}
					//if the request is an atomic APPEND, perform the atomic append
				} else if ((r.getRequestType()).equals(ATOMIC_APPEND)) {

					for (int cs : r.getChunkservers()) {
						//locate the primary
						if(r.getPrimaryID() == cs){
							try {
								//print out the information of the atomic append and make the request
								System.out.println("Trying to connect to chunkserver " + (cs));
								System.out.println(r.getFullPath());
								System.out.println(r.payload);
								System.out.println(r.getLength());
								System.out.println(r.getWithSize());
								System.out.println(chunkservers.keySet());
								if (chunkservers.get(cs).atomicAppend(r.getFullPath(), r.getPayload(),r.getLength(), r.getWithSize())) {
									System.out.println("Successful atomic append");
								} else {
									System.out.println("Failed atomic append");
								}
							} catch (RemoteException e) {
								//								e.printStackTrace();
								System.out.println("Failed to connect to chunkserver for atomic append");
							}
						}
					}
				//if the request is a read, perform the request
				} else if ((r.getRequestType()).equals(READ)) {
					//for (int cs : r.getChunkservers()) {
					Random rand = new Random();
					//pick a random chunkserver from the available chunkservers using rand
					int randIndex = Math.abs((rand.nextInt() % r.getChunkservers().size()));
					System.out.println("Reading from chunkserver " + r.getChunkservers().get(randIndex));
					try {
						//print the information of the request and get the results.
						byte[] result = chunkservers.get(r.getChunkservers().get(randIndex)).read(r.getFullPath(), r.getOffset(),r.getLength());
						System.out.println(r.reqID);
						System.out.println(r.fullPath);
						System.out.println(r.destination);
						File localDest = new File(r.destination);
						if (localDest.exists()){
							System.err.println("Local file destination already exist for read.");
						}
						else{
							//create the new file with which to put the data from the read.
							localDest.createNewFile();
							FileOutputStream fos = new FileOutputStream(localDest);
							fos.write(result);
							fos.flush();
							fos.close();
						}
						pendingRequests.remove(r);
					} catch (RemoteException e) {
						System.out.println("Failed to connect to chunkserver for read");
					}
					catch(FileNotFoundException fnfe){
						System.err.println("Local destination file for read unable to be created.");
					}
					catch(IOException ioe){
						System.err.println("Error creating or writing to local file for read ouput.");
						pendingRequests.remove(r);

					}
				//READ COMPLETELY
				} else if ((r.getRequestType()).equals(READ_COMPLETELY)) {
					Random rand = new Random();
					//generate a random chunkserver from the list of chunkservers
					int randIndex = Math.abs((rand.nextInt() % r.getChunkservers().size()));
					System.out.println("Reading completely from chunkserver " + r.getChunkservers().get(randIndex));
					try {
						//print the information of the request
						System.out.println("This are all chunkserver that client can read form: "+r.getChunkservers().toString());
						byte[] result = chunkservers.get(r.getChunkservers().get(randIndex)).readCompletely(r.getFullPath());
						System.out.println(r.reqID);
						System.out.println(r.fullPath);
						System.out.println(r.destination);
						File localDest = new File(r.destination);
						//if the file destination already exists print an error
						if (localDest.exists()){
							System.err.println("Local file destination already exist for read completely.");
						}
						else{
							//create the file with which to put the read data.
							localDest.createNewFile();
							FileOutputStream fos = new FileOutputStream(localDest);
							fos.write(result);
							fos.flush();
							fos.close();
						}
						pendingRequests.remove(r);
					} catch (RemoteException e) {
						System.out.println("Failed to connect to chunkserver for read completely");
					}
					catch(FileNotFoundException fnfe){
						System.err.println("Local destination file for read completely unable to be created.");
					}
					catch(IOException ioe){
						System.err.println("Error creating or writing to local file for read completely ouput.");
						pendingRequests.remove(r);
					}
				//get the number of files, TEST 7
				}else if ((r.getRequestType()).equals(NUM_FILES)) {
					//for (int cs : r.getChunkservers()) {
					Random rand = new Random();
					//pick a random chunkserver that has the file and is available
					int randIndex = Math.abs((rand.nextInt() % r.getChunkservers().size()));
					System.out.println("Reading number of files from chunkserver " + r.getChunkservers().get(randIndex));
					try {
						//print the information of the request
						System.out.println("This are all chunkserver that client can get information form: "+r.getChunkservers().toString());
						byte[] result = chunkservers.get(r.getChunkservers().get(randIndex)).numFiles(r.getFullPath());
						System.out.println(r.reqID);
						System.out.println(r.fullPath);
						pendingRequests.remove(r);
						ByteBuffer wrapped = ByteBuffer.wrap(result);
						int resInt = wrapped.getInt();
						System.out.println("Number of separate files are: " + resInt);
					} catch (RemoteException e) {
						System.out.println("Failed to connect to chunkserver for read completely");
					}
					catch(IOException ioe){
						System.err.println("Error creating or writing to local file for read completely ouput.");
						pendingRequests.remove(r);
					}
				}
				else {
					System.out.println("Error. Request type not found.");
				}
				pendingRequests.remove(r);
			}			
		}
	}

	private class MasterState extends Thread{
		Semaphore stateChanged;
		private MasterState() {

		}

		public void run() {
			boolean goOn = true;
			stateChanged = new Semaphore(1, true);
			while(goOn) {
				try {
					stateChanged.acquire();
					connectToMaster();
					master.connectToClient(clientID);
				} catch (InterruptedException e) {
					System.out.println("Could not acquire semaphore in Master Check.");
				} catch (RemoteException e) {
					System.out.println("Master connection to Client Failed");
				}
			}
		}
		public void stateChanged() {
			stateChanged.release();
		}

	}
}
