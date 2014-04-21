package Client;


import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;

import Interfaces.ClientInterface;
import Interfaces.MasterInterface;

public class Client implements ClientInterface{
	ArrayList<ClientMetaDataItem> clientMetaDataArray;
	List<Request> pendingRequests;
	int clientID;
	MasterInterface master;
	int count;
	Semaphore countLock = new Semaphore (1, true);

	public Client(int ID) {
		clientMetaDataArray = new ArrayList<ClientMetaDataItem>();	
		pendingRequests = Collections.synchronizedList(new ArrayList<Request>());
		clientID = ID;
		count = 0;

	}



	public void createFile(String Path, String fileName ,int numReplicas)throws RemoteException {
		try {
			master.createFile(Path, fileName, numReplicas, clientID);
		}
		catch(RemoteException e){
			e.printStackTrace();
		}

	}

	public void deleteFileMaster(String chunkhandle) throws RemoteException {
		try {
			master.deleteFileMaster(chunkhandle, clientID);
		}
		catch(RemoteException e){
			e.printStackTrace();
		}
	}	

	public void deleteDirectory(String path) throws RemoteException {
		try {
			master.deleteDirectory(path, clientID);
		}
		catch(RemoteException e){
			e.printStackTrace();
		}
	}

	public void append(String chunkhandle) throws RemoteException { //if no metadata is stored on the chunkhandle, ask master for locations
		try {
			master.append(chunkhandle, clientID);
			countLock.acquire();
			count++;
			Request r = new Request("append", chunkhandle, count);
			pendingRequests.add(r);
			countLock.release();	
		}
		catch(RemoteException e){
			e.printStackTrace();
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void atomicAppend(String chunkhandle) throws RemoteException {
		try {
			master.atomicAppend(chunkhandle, clientID);
			countLock.acquire();
			count++;
			Request r = new Request("atomicAppend", chunkhandle, count);
			pendingRequests.add(r);
			countLock.release();
		}
		catch(RemoteException e){
			e.printStackTrace();
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void read(String chunkhandle) throws RemoteException {
		int index = alreadyInClientMetaData(chunkhandle); // method returns index of item if the chunkhandle already exists, otherwise it returns -1;
		if (index > -1) { //if the index is found, do not contact master.
			try {
				countLock.acquire();
				count++;
				//this constructor adds the request knowing that it already has the server locations
				ClientMetaDataItem i = (ClientMetaDataItem) clientMetaDataArray.get(index);
				Request r = new Request("append", chunkhandle, count, i.chunkservers);
				pendingRequests.add(r);
				countLock.release();	
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		else {
			try {
				master.read(chunkhandle, clientID);
				countLock.acquire();
				count++;
				Request r = new Request("read", chunkhandle, count);
				pendingRequests.add(r);
				countLock.release();
			}
			catch(RemoteException e){
				e.printStackTrace();
			}
			catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

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
		//ClientMetaDataItem temp = new ClientMetaDataItem(chunkhandle, ID, chunkservers);
		//clientMetaDataArray.add(temp);

		//Go through the pendingRequests array to find request with the matching reqID.
		for (int i = 0; i < pendingRequests.size(); i++) {
			Request r = pendingRequests.get(i);
			//if the reqID's are matching
			if (reqID == r.ID) {
				r.setCS(chunkservers);
				r.setReceived();
				boolean exists = false; //boolean to check if this chunkhandle already exists in the Client's metadata.
				for (int j = 0; j < clientMetaDataArray.size(); j++) {
					ClientMetaDataItem c = clientMetaDataArray.get(j);
					//if the chunkhandle is found, exit the loop
					if (c.chunkhandle.equals(chunkhandle)) {
						exists = true;
						c.setID(ID);  //Update the primary lease in case it is different/ has changed
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
		Request r;
		
	}


}
