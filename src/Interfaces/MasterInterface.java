package Interfaces;

import java.rmi.*;

public interface MasterInterface extends Remote {
	//functions called by the client
	public void setupHost() throws RemoteException;
	public void setupClient() throws RemoteException;
	public void createFile(String path, String fileName, int numReplicas, int clientID) throws RemoteException;
	public void deleteFileMaster(String chunkhandle, int clientID) throws RemoteException;
	public void createDirectory(String path, int clientID) throws RemoteException;
	public void deleteDirectory(String path, int clientID) throws RemoteException;
	public void append(String chunkhandle, int clientID, int reqID) throws RemoteException;
	public void atomicAppend(String chunkhandle, int clientID, int reqID) throws RemoteException;
	public void read(String chunkhandle, int clientID, int reqID) throws RemoteException;
	//functions called by the chunkserver
	public void heartbeat(int CSID) throws RemoteException;
}
