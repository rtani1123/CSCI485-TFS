package Client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.MalformedURLException;
import java.net.Socket;
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
 * place of contacting the Master to save time.
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
	// unique request IDs

	// requestType strings
	public static final String APPEND = "append";
	public static final String ATOMIC_APPEND = "atomicAppend";
	public static final String READ = "read";
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
		connectToMaster();
		master.connectToClient(clientID);
	}

	// Master calls Client methods -> MASTERCLIENT
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

	// Client Calls Master code -> CLIENTMASTER
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
	public void setChunkservers(HashMap<Integer, ChunkserverInterface> chunkservers) {
		this.chunkservers = chunkservers;
		for(Map.Entry<Integer, ChunkserverInterface> entry : this.chunkservers.entrySet()) {
			connectToChunkserver(entry.getKey());
			System.out.println("Connection to " + entry.getKey() + " successful.");
			try {
				entry.getValue().connectToClient(clientID);
				System.out.println("Chunkserver " + entry.getKey() + " connected to Client " + clientID);
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public void connectToChunkserver(Integer index) {
		try {
			System.setSecurityManager(new RMISecurityManager());
			ChunkserverInterface tempCS = null;

			if(index == 1)
				tempCS = (ChunkserverInterface)Naming.lookup("rmi://dblab-36.vlab.usc.edu:123/CHUNK" + index.toString());
			else if(index == 2)
				tempCS = (ChunkserverInterface)Naming.lookup("rmi://dblab-05.vlab.usc.edu:124/CHUNK" + index.toString());
			else if(index == 3)
				tempCS = (ChunkserverInterface)Naming.lookup("rmi://dblab-29.vlab.usc.edu:125/CHUNK" + index.toString());

			// TODO: Change this to handle multiple chunkservers.
			// chunkservers.put(1, tempCS);
			chunkservers.put(index,tempCS);
			/*
			 * ChunkServer FUNCTION HOST implementation
			 */
			System.out.println("Connection to Chunkserver " + index
					+ " Success");

		} catch (Exception re) {
			System.out.println("Failure to connect to Chunkserver " + index);
		}
	}



	// called by the application
	public void createFile(String Path, String fileName, int numReplicas)
			throws RemoteException {
		try {
			master.createFile(Path, fileName, numReplicas, clientID);
		} catch (RemoteException e) {
			System.out.println("Could not connect to master to create file.");
		}
	}

	public void createDirectory(String path) throws RemoteException {
		try {
			master.createDirectory(path, clientID);
		}
		catch(RemoteException e){
			System.out.println("Could not connect to master to create file.");
		}
	}

	// called by the application
	public void deleteFileMaster(String chunkhandle) throws RemoteException {
		try {
			master.deleteFileMaster(chunkhandle, clientID);
		} catch (RemoteException e) {
			System.out.println("Could not connect to master to delete file.");
		}
	}

	// called by the application
	public void deleteDirectory(String path) throws RemoteException {
		try {
			master.deleteDirectory(path, clientID);
		} catch (RemoteException e) {

			System.out.println("Could not connect to master to delete directory.");

		}
	}

	// called by the application
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
			Request r = new Request(APPEND, chunkhandle, count);
			r.setData(data);
			r.setLength(length);
			r.setOffset(offset);
			r.setWithSize(withSize);
			pendingRequests.add(r);
			master.append(chunkhandle, clientID, count);
			countLock.release();
		} catch (RemoteException e) {
			System.out.println("Could not connect to master to append.");
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
			System.out.println("Interrupted Exception in append method");
		}
	}

	// called by the application
	public void atomicAppend(String chunkhandle, int length, byte[] data, boolean withSize) throws RemoteException {
		int id = -1;
		try {
			countLock.acquire();
			id = ++count;
			Request r = new Request(ATOMIC_APPEND, chunkhandle, count);
			r.setData(data);
			r.setLength(length);
			r.setWithSize(withSize);
			pendingRequests.add(r);
			master.atomicAppend(chunkhandle, clientID, count);
			countLock.release();
		} catch (RemoteException e) {
			System.out.println("Could not connect to master to atomic append.");
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

	// called by the application
	public void read(String chunkhandle, int offset, int length, String destination)
			throws RemoteException {
		int index = alreadyInClientMetaData(chunkhandle); // method returns
		// index of item if
		// the chunkhandle
		// already exists,
		// otherwise it
		// returns -1;
		if (index > -1) { // if the index is found, do not contact master.
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
		} else {
			int id = -1;
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
				System.out.println("Could not connect to master to atomic append.");
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

	// Method called by master giving chunkhandle and chunkservers
	public void passMetaData(String chunkhandle, int pID, ArrayList<Integer> chunkserversList, int reqID) {
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
								// update the primary lease in case it is different/has changed
								break;
							}
						}
						// if the chunkhandle was not already in the metadata, add it along with its chunkservers
						if (!exists) {
							clientMetaDataArray.add(new ClientMetaDataItem(chunkhandle, pID, chunkserversList));
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

	// return index of metadata for chunkhandle if already known
	// otherwise return -1
	private int alreadyInClientMetaData(String chunkhandle) {
		for (int i = 0; i < clientMetaDataArray.size(); i++) {
			if (chunkhandle.equals(clientMetaDataArray.get(i).chunkhandle))
				return i;
		}
		return -1;
	}

	// call this to contact chunkservers
	private void contactChunks(int rID) {
		System.out.println("contacting chunks... ");
		for (int i = 0; i < pendingRequests.size(); i++) {
			Request r = (Request) pendingRequests.get(i);
			if (r.getReqID() == rID) {
				if ((r.getRequestType()).equals(APPEND)) {
					System.out.println("chunkservers for append in contactCS func: "+r.getChunkservers());
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
				} else if ((r.getRequestType()).equals(ATOMIC_APPEND)) {

					for (int cs : r.getChunkservers()) {
						if(r.getPrimaryID() == cs){
							try {
								System.out.println("Trying to connect to chunkserver " + (cs));
								if (chunkservers.get(cs).atomicAppend(r.getFullPath(), r.getPayload(),r.getLength(), r.getWithSize())) {
									System.out.println("Successful atomic append");
								} else {
									System.out.println("Failed atomic append");
								}
							} catch (RemoteException e) {
								e.printStackTrace();
								System.out.println("Failed to connect to chunkserver for atomic append");
							}
						}
					}
				} else if ((r.getRequestType()).equals(READ)) {
					//for (int cs : r.getChunkservers()) {
					Random rand = new Random();
					int randCS = Math.abs((rand.nextInt() % r.getChunkservers().size()));
					randCS++;
					System.out.println("Reading from chunkserver " + randCS);
					try {
						byte[] result = chunkservers.get(randCS).read(r.getFullPath(), r.getOffset(),r.getLength());
						System.out.println(r.reqID);
						System.out.println(r.fullPath);
						System.out.println(r.destination);
						File localDest = new File(r.destination);
						if (localDest.exists()){
							System.err.println("Local file destination already exist for read.");
						}
						else{
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
				} else {
					System.out.println("Error. Request type not found.");


				}
				pendingRequests.remove(r);
			}			
		}
	}

}
