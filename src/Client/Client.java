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

	public void append(String chunkhandle) throws RemoteException {
		try {
			master.append(chunkhandle, clientID);
			countLock.acquire();
			Request r = new Request("append", chunkhandle, count);
			count++;
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
			Request r = new Request("atomicAppend", chunkhandle, count);
			count++;
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
		try {
			master.read(chunkhandle, clientID);
			countLock.acquire();
			Request r = new Request("read", chunkhandle, count);
			count++;
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

	public void requestStatus(String requestType, String fullPath, boolean succeeded, int ID) throws RemoteException {

		if (succeeded) {
			System.out.println("Request to" + requestType + " " + fullPath + "succeeded.");
		}
		else {
			System.out.println("Request to" + requestType + " " + fullPath + "failed.");
		}

	}

	public void passMetaData(String chunkhandle, int ID, ArrayList<Integer> chunkservers) {
		ClientMetaDataItem temp = new ClientMetaDataItem(chunkhandle, ID, chunkservers);
		clientMetaDataArray.add(temp);


	}
	//check for chunkhandle


}
