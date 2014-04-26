package Interfaces;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * Use this interface to connect to the Chunkservers.
 * <p>Methods from this interface are called by the Master, Clients, and other Chunkservers.</p>
 * <table style="border-style:solid; border-width:1px;">
 * <tr><td style="border-style:solid; border-width:1px;">Methods called by the Master:</td>
 * <td style="border-style:solid; border-width:1px;"><ul>
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
 * </ul></td></tr>
 * <tr><td style="border-style:solid; border-width:1px;">Methods called by Clients:</td>
 * <td style="border-style:solid; border-width:1px;"><ul>
 * <li>boolean atomicAppend(String, byte[], int, boolean)</li>
 * <li>byte[] read(String, int, int)</li>
 * <li>boolean append(String, byte[], int, int, boolean)</li>
 * </ul></td></tr>
 * <tr><td style="border-style:solid; border-width:1px;">Methods called by Chunkservers:</td>
 * <td style="border-style:solid; border-width:1px;"><ul>
 * <li>void setupChunkserverHost()</li>
 * <li>void connectToMaser()</li>
 * <li>boolean atomicAppendSecondary(String, byte[], int, boolean, long)</li>
 * <li>byte[] readCompletely(String)</li>
 * </ul></td></tr></table>
 */
public interface ChunkserverInterface extends Remote{
	//functions called by the master
	/**
	 * Attempts to establish the RMI connection to all other Chunkservers
	 * @throws RemoteException
	 */
	public void setupChunkserverHost() throws RemoteException;
	/**
	 * Attempts to establish RMI connection to Master
	 * @throws RemoteException
	 */
	public void connectToMaster() throws RemoteException;
	/**
	 * Attempts to establish RMI connection to a specified Client
	 * @param id	Client ID
	 * @throws RemoteException
	 */
	public void connectToClient(Integer id) throws RemoteException;
	/**
	 * Attempts to establish the RMI connection to a specified Chunkserver
	 * @param id	Chunkserver ID
	 * @throws RemoteException
	 */
	public void connectToChunkserver(Integer id) throws RemoteException;
	/**
	 * Sets CSMetadata primary lease times as well as appropriate secondary ChunkServers
	 * @param chunkhandle	full path of the file
	 * @param CServers	list of secondary Chunkserver IDs
	 * @throws RemoteException
	 */
	public void primaryLease(String chunkhandle, List<Integer> CServers) throws RemoteException;
	/**
	 * Creates a file in the chunkserver and updates CSMetadata if successful
	 * @param chunkhandle	full path of the file
	 * @return	true if and only if the operation succeeded; false otherwise
	 * @throws RemoteException
	 */
	public boolean createFile(String chunkhandle) throws RemoteException;
	/**
	 * Creates a directory in the chunkserver and updates CSMetadata if successful
	 * @param chunkhandle	full path of the file
	 * @return	true if and only if the operation succeeded; false otherwise
	 * @throws RemoteException
	 */
	public boolean createDirectory(String chunkhandle) throws RemoteException;
	/**
	 * Deletes a file in the chunkserver and updates CSMetadata if successful
	 * @param chunkhandle	full path of the file
	 * @return	true if and only if the operation succeeded; false otherwise
	 * @throws RemoteException
	 */
	public boolean deleteFile(String chunkhandle) throws RemoteException;
	/**
	 * Deletes a directory in the chunkserver and updates CSMetadata if successful
	 * @param chunkhandle	full path of the file
	 * @return	true if and only if the operation succeeded; false otherwise
	 * @throws RemoteException
	 */
	public boolean deleteDirectory(String chunkhandle) throws RemoteException;
	/**
	 * Updates file data from another Chunkserver
	 * @param chunkhandle	full path of the file
	 * @param sourceID	ID of the Chunkserver used to update the data
	 * @throws RemoteException
	 */
	public void fetchAndRewrite(String chunkhandle, int sourceID) throws RemoteException;
	/**
	 * Messages the Master with its Chunkserver ID
	 * @throws RemoteException
	 */
	public void heartbeat() throws RemoteException;
	/**
	 * Writes to the end of a specified file and updates CSMetadata if successful
	 * @param chunkhandle	full path of the file
	 * @param payload	data to be written to the file
	 * @param length	length of the file
	 * @param withSize	if true - expect byte array with haystack format; if false - byte array is only payload
	 * @return	true if and only if the operation succeeded; false otherwise
	 * @throws RemoteException
	 */
	public boolean atomicAppend(String chunkhandle, byte[] payload, int length, boolean withSize) throws RemoteException;
	/**
	 * Reads from a specified offset in a specified file
	 * @param chunkhandle	full path of the file
	 * @param offset	offset in file at which to begin read
	 * @param length	length of the file
	 * @return	true if and only if the operation succeeded; false otherwise
	 * @throws RemoteException
	 */
	public byte[] read(String chunkhandle, int offset, int length) throws RemoteException;
	/**
	 * Writes at a specified offset in a specified file and updates CSMetadata if successful
	 * @param chunkhandle	full path of the file
	 * @param payload	data to be written to the file
	 * @param length	length of the file
	 * @param offset	offset in file at which to begin write
	 * @param withSize	if true - expect byte array with haystack format; if false - byte array is only payload
	 * @return	true if and only if the operation succeeded; false otherwise
	 * @throws RemoteException
	 */
	public boolean append(String chunkhandle, byte[] payload, int length, int offset, boolean withSize) throws RemoteException;
	/**
	 * Writes to the end of a specified file and updates CSMetadata if successful
	 * @param chunkhandle	full path of the file
	 * @param payload	data to be written to the file
	 * @param length	length of the file
	 * @param withSize	if true - expect byte array with haystack format; if false - byte array is only payload
	 * @param offset	offset in file at which to begin write
	 * @return	true if and only if the operation succeeded; false otherwise
	 * @throws RemoteException
	 */
	public boolean atomicAppendSecondary(String chunkhandle, byte[] payload, int length, boolean withSize, long offset) throws RemoteException;
	/**
	 * Reads an entire specified file
	 * @param chunkhandle	full path of the file
	 * @return	true if and only if the operation succeeded; false otherwise
	 * @throws RemoteException
	 */
	public byte[] readCompletely(String chunkhandle)throws RemoteException;
	/**
	 * Returns the number of separate payloads in a specified file with the haystack format
	 * @param chunkhandle	full path of the file
	 * @return	true if and only if the operation succeeded; false otherwise
	 * @throws RemoteException
	 */
	public byte[] numFiles(String chunkhandle)throws RemoteException;
}
