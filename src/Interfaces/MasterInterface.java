package Interfaces;

import java.rmi.*;

public interface MasterInterface extends Remote {
	public boolean createFile(String path, String fileName, int numReplicas) throws RemoteException;
	public boolean getXLock(String filePath) throws RemoteException;
	public void deleteFileMaster(String chunkhandle) throws RemoteException;
	public boolean deleteFileChunk(String path) throws RemoteException;
	public boolean createDirectory(String path) throws RemoteException;
	public void deleteDirectory(String path) throws RemoteException;
	public void append(String chunkhandle, int offset, int length, byte[] data) throws RemoteException;
	public void atomicAppend(String chunkhandle, int length, byte[] data) throws RemoteException;
	public void atomicAppendWithSize(String chunkhandle, int length, byte[] data) throws RemoteException;
	public byte[] read(String chunkhandle, int offset, int length) throws RemoteException;
	public byte[] readCompletely(String chunkhandle) throws RemoteException;
	public long getFileSize(String chunkhandle) throws RemoteException;
}
