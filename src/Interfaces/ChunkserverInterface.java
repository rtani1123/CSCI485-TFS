package Interfaces;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Map;


public interface ChunkserverInterface extends Remote{
	//functions called by the master
	public void setupChunkserverHost() throws RemoteException;
	public void connectToMaster() throws RemoteException;
	public void connectToClient() throws RemoteException;
	public void connectToChunkserver(Integer index) throws RemoteException;
	public Map<String, Long> refreshMetadata() throws RemoteException;
	public void primaryLease(String chunkhandle, ArrayList<Integer> CServers) throws RemoteException;
	public boolean createFile(String chunkhandle) throws RemoteException;
	public boolean createDirectory(String chunkhandle) throws RemoteException;
	public boolean deleteFile(String chunkhandle) throws RemoteException;
	public boolean deleteDirectory(String chunkhandle) throws RemoteException;
	public void fetchAndRewrite(String chunkhandle, int sourceID) throws RemoteException;
	public void heartbeat() throws RemoteException;
	//functions called by the client
	public boolean atomicAppend(String chunkhandle, byte[] payload, int length, boolean withSize) throws RemoteException;
	public byte[] read(String chunkhandle, int offset, int length) throws RemoteException;
	public boolean append(String chunkhandle, byte[] payload, int length, int offset, boolean withSize) throws RemoteException;
	//functions called by other CS
	public boolean atomicAppendSecondary(String chunkhandle, byte[] payload, int length, boolean withSize, long offset) throws RemoteException;
	byte[] readCompltely(String chunkhandle);
}
