package Client;


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
import java.util.List;
import java.util.concurrent.Semaphore;

import Chunkserver.ChunkServer;
import Interfaces.ChunkserverInterface;
import Interfaces.ClientInterface;
import Interfaces.MasterInterface;
import Master.Master;

public class Client extends UnicastRemoteObject implements ClientInterface{
	ArrayList<ClientMetaDataItem> clientMetaDataArray;		// locations of replicas and primary lease
	List<ChunkserverInterface> chunkservers;				// chunkservers to contact
	List<Request> pendingRequests;							// application request info for append, atomic append, and read 
	int clientID;											// ID of this client
	MasterInterface master;									// master to contact
	int count;												// used to create unique request IDs
	Semaphore countLock = new Semaphore (1, true);			// semaphore for creating unique request IDs

	// requestType strings
	public static final String APPEND = "append";
	public static final String ATOMIC_APPEND = "atomicAppend";
	public static final String READ = "read";


	// constructor, takes an ID for the client
	public Client(int ID) throws RemoteException {
		clientMetaDataArray = new ArrayList<ClientMetaDataItem>();	
		pendingRequests = Collections.synchronizedList(new ArrayList<Request>());
		clientID = ID;
		count = 0;
		chunkservers = Collections.synchronizedList(new ArrayList<ChunkserverInterface>());
		
		setupClientHost();
	}

	//Master calls Client methods -> MASTERCLIENT
	public void setupClientHost() throws RemoteException {
		try {
			System.setSecurityManager(new RMISecurityManager());
			Registry registry = LocateRegistry.createRegistry(1099);
			Naming.rebind("rmi://dblab-43.vlab.usc.edu/CLIENT", this);
			System.out.println("Client Host Setup Success");
		} catch (MalformedURLException re) {
			System.out.println("Bad connection");
			re.printStackTrace();
		} catch (RemoteException e) {
			System.out.println("Bad connection");
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		//now run Master.
	}

	//Client Calls Master code -> CLIENTMASTER
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
			re.printStackTrace();
		}
	}
	
	public void connectToChunkserver(Integer index) {
		try {
			System.setSecurityManager(new RMISecurityManager());
			ChunkserverInterface tempCS;

			tempCS = (ChunkserverInterface)Naming.lookup("rmi://dblab-18.vlab.usc.edu/CHUNK" + index.toString());
			
			//TODO: Change this to handle multiple chunkservers.
			//chunkservers.put(1, tempCS);
			chunkservers.add(tempCS);
			/*
			 * ChunkServer FUNCTION HOST implementation
			 */
			System.out.println("Connection to Chunkserver " + index + " Success");

		} catch(Exception re) {
			re.printStackTrace();
		}
	}
	// ***THIS WILL NEED TO BE UPDATED***
	public void setUpConnections() throws RemoteException{
		chunkservers.add(new ChunkServer());
		chunkservers.add(new ChunkServer());
		chunkservers.add(new ChunkServer());
		
		master = new Master();
	}

	// called by the application
	public void createFile(String Path, String fileName, int numReplicas)throws RemoteException {
		try {
			master.createFile(Path, fileName, numReplicas, clientID);
		}
		catch(RemoteException e){
			System.out.println("Could not connect to master to create file.");
			e.printStackTrace();
		}
	}

	// called by the application
	public void deleteFileMaster(String chunkhandle) throws RemoteException {
		try {
			master.deleteFileMaster(chunkhandle, clientID);
		}
		catch(RemoteException e){
			System.out.println("Could not connect to master to delete file.");
			e.printStackTrace();
		}
	}	

	// called by the application
	public void deleteDirectory(String path) throws RemoteException {
		try {
			master.deleteDirectory(path, clientID);
		}
		catch(RemoteException e){
			System.out.println("Could not connect to master to delete directory.");
			e.printStackTrace();
		}
	}

	// called by the application
	public void append(String chunkhandle, int offset, int length, byte[] data, boolean withSize) throws RemoteException { //if no metadata is stored on the chunkhandle, ask master for location
		int id = -1;
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
		}
		catch(RemoteException e){
			System.out.println("Could not connect to master to append.");
			e.printStackTrace();
			// Remove request if could not connect to master
			int index = -1;
			for (int x = 0; x < pendingRequests.size(); x++) {
				if ((pendingRequests.get(x)).getID() == id) {
					index = x;
					break;
				}
			}
			pendingRequests.remove(index);

		}
		catch (InterruptedException e) {
			e.printStackTrace();
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
		}
		catch(RemoteException e){
			System.out.println("Could not connect to master to atomic append.");
			e.printStackTrace();
			// Remove request if could not connect to master
			int index = -1;
			for (int x = 0; x < pendingRequests.size(); x++) {
				if ((pendingRequests.get(x)).getID() == id) {
					index = x;
					break;
				}
			}
			pendingRequests.remove(index);
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	// called by the application
	public void read(String chunkhandle, int offset, int length) throws RemoteException {
		int index = alreadyInClientMetaData(chunkhandle); // method returns index of item if the chunkhandle already exists, otherwise it returns -1;
		if (index > -1) { //if the index is found, do not contact master.
			try {
				countLock.acquire();
				count++;
				//this constructor adds the request knowing that it already has the server locations
				ClientMetaDataItem i = (ClientMetaDataItem) clientMetaDataArray.get(index);
				Request r = new Request(READ, chunkhandle, count, i.chunkservers);
				r.setLength(length);
				r.setOffset(offset);
				pendingRequests.add(r);
				countLock.release();	
				contactChunks(r.getID());
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		else {
			int id = -1;
			try {	
				countLock.acquire();
				id = ++count;
				pendingRequests.add(new Request(READ, chunkhandle, count));
				master.read(chunkhandle, clientID, count);
				countLock.release();
			}
			catch(RemoteException e){
				System.out.println("Could not connect to master to atomic append.");
				e.printStackTrace();
				// Remove request if could not connect to master
				int ind = -1;
				for (int x = 0; x < pendingRequests.size(); x++) {
					if (pendingRequests.get(x).getID() == id) {
						ind = x;
						break;
					}
				}
				pendingRequests.remove(ind);
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	// called by master
	public void requestStatus(String requestType, String fullPath, boolean succeeded, int ID) throws RemoteException {
		if (succeeded) {
			System.out.println("Request to" + requestType + " " + fullPath + "succeeded.");
		}
		else {
			System.out.println("Request to" + requestType + " " + fullPath + "failed.");
		}
	}

	//Method called by master giving chunkhandle and chunkservers
	public void passMetaData(String chunkhandle, int ID, ArrayList<Integer> chunkservers, int reqID) {
		//reqID of -1 is used for functions such as Creates and Deletes which are not stored in the pendingRequests.
		if (reqID != -1) {
			//Go through the pendingRequests array to find request with the matching reqID.
			// Save locations of replicates and/or update primary lease
			for (int i = 0; i < pendingRequests.size(); i++) {
				Request r = pendingRequests.get(i);
				//if the reqID's are matching
				if (reqID == r.getID()) {
					r.setCS(chunkservers);
					r.setReceived();
					boolean exists = false; //boolean to check if this chunkhandle already exists in the Client's metadata.
					for (int j = 0; j < clientMetaDataArray.size(); j++) {
						//if the chunkhandle is found, exit the loop
						if ((clientMetaDataArray.get(j)).equals(chunkhandle)) {
							exists = true;
							(clientMetaDataArray.get(j)).setID(ID);  //Update the primary lease in case it is different/ has changed
							break;
						}
					}
					//if the chunkhandle was not already in the metadata, add it along with its chunkservers
					if (!exists) {
						clientMetaDataArray.add(new ClientMetaDataItem(chunkhandle, ID, chunkservers));
					}
					//once the corresponding ReqID is found, break out of the outer loop.
					break;

				}
			}
			contactChunks(reqID);
		}
	}
	
	// return index of metadata for chunkhandle if already known
	// otherwise return -1
	private int alreadyInClientMetaData(String chunkhandle) {
		for (int i = 0; i < clientMetaDataArray.size(); i++) {
			if (chunkhandle.equals(clientMetaDataArray.get(i).chunkhandle)) return i;
		}
		return -1;
	}

	// call this to contact chunkservers
	private void contactChunks(int rID) {
		// retrieve request
		for (int i = 0; i < pendingRequests.size(); i++) {
			Request r = (Request) pendingRequests.get(i);
			if (r.getID() == rID) {
				if((r.getRequestType()).equals(APPEND)){
					for(int cs:r.getChunkservers()){
						try {
							if(chunkservers.get(cs).append(r.getFullPath(), r.getPayload(), r.getLength(), r.getOffset(), r.getWithSize())){
								System.out.println("Successful append");
								// call application to print to command line
								pendingRequests.remove(r);
							}
							else{
								System.out.println("Failed append");
								// call application to print to command line
								pendingRequests.remove(r);
							}
						} catch (RemoteException e) {
							System.out.println("Failed to connect to chunkserver for append");
							pendingRequests.remove(r);
							e.printStackTrace();
						}
					}
				}
				else if((r.getRequestType()).equals(ATOMIC_APPEND)){
					for(int cs:r.getChunkservers()){
						try {
							if(chunkservers.get(cs).atomicAppend(r.getFullPath(), r.getPayload(), r.getLength(), r.getWithSize())){
								System.out.println("Successful atomic append");
								// call application to print to command line
								pendingRequests.remove(r);
							}
							else{
								System.out.println("Failed atomic append");
								// call application to print to command line
								pendingRequests.remove(r);
							}
						} catch (RemoteException e) {
							System.out.println("Failed to connect to chunkserver for atomic append");
							pendingRequests.remove(r);
							e.printStackTrace();
						}
					}
				}
				else if((r.getRequestType()).equals(READ)){
					for(int cs:r.getChunkservers()){
						try {
							System.out.println(chunkservers.get(cs).read(r.getFullPath(), r.getOffset(), r.getLength()));
							pendingRequests.remove(r);
						} catch (RemoteException e) {
							System.out.println("Failed to connect to chunkserver for read");
							pendingRequests.remove(r);
							e.printStackTrace();
						}
					}
				}
				else{
					System.out.println("Error. Request type not found.");
				}
			}
		}
	}

}
