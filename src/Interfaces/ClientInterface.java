package Interfaces;

import java.rmi.*;
import java.util.ArrayList;

public interface ClientInterface extends Remote {
	//functions called by master
	public void setupClientHost() throws RemoteException;
	public void connectToMaster() throws RemoteException;
	public void requestStatus(String requestType, String fullPath, boolean succeeded, int ID) throws RemoteException;
	public void passMetaData(String chunkhandle, int ID, ArrayList<Integer> chunkservers, int reqID) throws RemoteException;//chunkhandle, int id of primary, list of chunkservers

	// functions called by application
	public void createFile(String Path, String fileName, int numReplicas)throws RemoteException;
	public void createDirectory(String path) throws RemoteException;
	public void deleteFileMaster(String chunkhandle) throws RemoteException;
	public void deleteDirectory(String path) throws RemoteException;
	public void append(String chunkhandle, int offset, int length, byte[] data, boolean withSize) throws RemoteException;
	public void atomicAppend(String chunkhandle, int length, byte[] data, boolean withSize) throws RemoteException;
	public void read(String chunkhandle, int offset, int length) throws RemoteException;
}
