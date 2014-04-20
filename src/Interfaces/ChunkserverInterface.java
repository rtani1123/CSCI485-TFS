package Interfaces;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Map;


public interface ChunkserverInterface extends Remote{
	//functions called by the master
	public Map<String, Long> refreshMetadata() throws RemoteException;
	public void primaryLease(String chunkhandle) throws RemoteException;
	public boolean createFile(String chunkhandle) throws RemoteException;
	public boolean createDirectory(String chunkhandle) throws RemoteException;
	public boolean deleteFile(String chunkhandle) throws RemoteException;
	public boolean deleteDirectory(String chunkhandle) throws RemoteException;
	//functions called by the client
	public boolean atomicAppend(String chunkhandle, byte[] payload, int length, boolean withSize) throws RemoteException;
	public byte[] read(String chunkhandle, int offset, int length) throws RemoteException;
	public boolean append(String chunkhandle, byte[] payload, int length, int offset, boolean withSize) throws RemoteException;
	//functions called by other CS
	public boolean atomicAppendSecondary(String chunkhandle, byte[] payload, int length, boolean withSize, int offset) throws RemoteException;
}
