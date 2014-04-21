package Client;


import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;

import Chunkserver.ChunkServer;
import Interfaces.ChunkserverInterface;
import Interfaces.ClientInterface;
import Interfaces.MasterInterface;

public class Client implements ClientInterface{
	ArrayList<ClientMetaDataItem> clientMetaDataArray;
	List<ChunkserverInterface> chunkservers;
	List<Request> pendingRequests;
	int clientID;
	MasterInterface master;
	int count;
	Semaphore countLock = new Semaphore (1, true);

	public static final String APPEND = "append";
	public static final String ATOMIC_APPEND = "atomicAppend";
	public static final String READ = "read";


	// constructor takes an ID for the client
	public Client(int ID) {
		clientMetaDataArray = new ArrayList<ClientMetaDataItem>();	
		pendingRequests = Collections.synchronizedList(new ArrayList<Request>());
		clientID = ID;
		count = 0;
		chunkservers = Collections.synchronizedList(new ArrayList<ChunkserverInterface>());
	}

	// ***THIS WILL NEED TO BE UPDATED***
	public void setUpChunkservers() throws RemoteException{
		chunkservers.add(new ChunkServer());
		chunkservers.add(new ChunkServer());
		chunkservers.add(new ChunkServer());
	}

	// called by the application
	public void createFile(String Path, String fileName, int numReplicas)throws RemoteException {
		try {
			master.createFile(Path, fileName, numReplicas, clientID);
		}
		catch(RemoteException e){
			e.printStackTrace();
		}
	}

	// called by the application
	public void deleteFileMaster(String chunkhandle) throws RemoteException {
		try {
			master.deleteFileMaster(chunkhandle, clientID);
		}
		catch(RemoteException e){
			e.printStackTrace();
		}
	}	

	// called by the application
	public void deleteDirectory(String path) throws RemoteException {
		try {
			master.deleteDirectory(path, clientID);
		}
		catch(RemoteException e){
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
			e.printStackTrace();
			int index = -1;
			for (int x = 0; x < pendingRequests.size(); x++) {
				if ((pendingRequests.get(x)).ID == id) {
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
			e.printStackTrace();
			int index = -1;
			for (int x = 0; x < pendingRequests.size(); x++) {
				if ((pendingRequests.get(x)).ID == id) {
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
				Request r = new Request(READ, chunkhandle, count);
				pendingRequests.add(r);
				master.read(chunkhandle, clientID, count);
				countLock.release();
			}
			catch(RemoteException e){
				e.printStackTrace();
				int ind = -1;
				for (int x = 0; x < pendingRequests.size(); x++) {
					Request r = (Request) pendingRequests.get(x);
					if (r.ID == id) {
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
						ClientMetaDataItem cmdi = new ClientMetaDataItem(chunkhandle, ID, chunkservers);
						clientMetaDataArray.add(cmdi);
					}
					//once the corresponding ReqID is found, break out of the outer loop.
					break;

				}
			}
			contactChunks(reqID);
		}
	}
	//check for chunkhandle

	private int alreadyInClientMetaData(String chunkhandle) {
		for (int i = 0; i < clientMetaDataArray.size(); i++) {
			ClientMetaDataItem  c = (ClientMetaDataItem) clientMetaDataArray.get(i);
			if (chunkhandle.equals(c.chunkhandle)) return i;
		}
		return -1;
	}

	private void contactChunks(int rID) {
		// retrieve request
		for (int i = 0; i < pendingRequests.size(); i++) {
			Request r = (Request) pendingRequests.get(i);
			if (r.ID == rID) {
				if((r.getRequestType()).equals(APPEND)){
					for(int cs:r.getChunkservers()){
						try {
							chunkservers.get(cs).append(r.getFullPath(), r.getPayload(), r.getLength(), r.getOffset(), r.getWithSize());
						} catch (RemoteException e) {
							System.out.println("Failed to connect to chunkserver for append");
							e.printStackTrace();
						}
					}
				}
				else if((r.getRequestType()).equals(ATOMIC_APPEND)){
					for(int cs:r.getChunkservers()){
						try {
							chunkservers.get(cs).atomicAppend(r.getFullPath(), r.getPayload(), r.getLength(), r.getWithSize());
						} catch (RemoteException e) {
							System.out.println("Failed to connect to chunkserver for atomic append");
							e.printStackTrace();
						}
					}
				}
				else if((r.getRequestType()).equals(READ)){
					for(int cs:r.getChunkservers()){
						try {
							chunkservers.get(cs).read(r.getFullPath(), r.getOffset(), r.getLength());
						} catch (RemoteException e) {
							System.out.println("Failed to connect to chunkserver for read");
							e.printStackTrace();
						}
					}
				}
			}
		}
	}
}
