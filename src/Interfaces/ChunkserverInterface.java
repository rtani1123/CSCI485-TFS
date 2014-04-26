package Interfaces;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * Use this interface to connect to the Chunkservers.
 * Methods from this interface are called by the Master, the Clients, and other Chunkservers.
 * Methods called by the Master:
 * <ul>
 * <li>void setupChunkserverHost()</li>
 * <li>void connectToMaser()</li>
 * <li>void connectToClient(Integer)</li>
 * <li>void connectToChunkserver(Integer)</li>
 * <li>void primaryLease(String, List<Integer>)</li>
 * <li>boolean createFile(String)</li>
 * <li>boolean createDirectory(String)</li>
 * <li>boolean deleteFile(String)</li>
 * <li>boolean deleteDirectory(String)</li>
 * <li>void fetchAndRewrite(String, int)</li>
 * <li>void heartbeat()</li>
 * </ul> 
 * Methods called by Clients:
 * <ul>
 * <li>boolean atomicAppend(String, byte[], int, boolean)</li>
 * <li>byte[] read(String, int, int)</li>
 * <li>boolean append(String, byte[], int, int, boolean)</li>
 * </ul>
 * Methods called by Chunkservers:
 * <ul>
 * <li>boolean atomicAppendSecondary(String, byte[], int, boolean, long)</li>
 * <li>byte[] readCompletely(String)</li>
 * </ul>
 */
public interface ChunkserverInterface extends Remote{
	//functions called by the master
	public void setupChunkserverHost() throws RemoteException;
	public void connectToMaster() throws RemoteException;
	public void connectToClient(Integer id) throws RemoteException;
	public void connectToChunkserver(Integer id) throws RemoteException;
	//public Map<String, Long> refreshMetadata() throws RemoteException;
	public void primaryLease(String chunkhandle, List<Integer> CServers) throws RemoteException;
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
	public byte[] readCompletely(String chunkhandle)throws RemoteException;
	public byte[] numFiles(String chunkhandle)throws RemoteException;
}
