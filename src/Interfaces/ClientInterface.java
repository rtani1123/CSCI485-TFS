package Interfaces;

import java.rmi.*;
import java.util.List;
import java.util.Map;

/**
 * Use this interface to connect to the Client.
 * <p>Methods from this interface are called by the Master and Application.</p>
 * <table style="border-style:solid; border-width:1px;">
 * <tr><td style="border-style:solid; border-width:1px;">Methods called by the Master:</td>
 * <td style="border-style:solid; border-width:1px;"><ul>
 * <li>void setupClientHost(Integer)</li>
 * <li>void connectToMaster()</li>
 * <li>void setChunkservers(Map<Integer, ChunkserverInterface>)</li>
 * <li>void connectToChunkserver(Integer)</li>
 * <li>void requestStatus(String, String, boolean, int)</li>
 * <li>passMetaData(String, int, List<Integer>, int)</li>
 * </ul></td></tr>
 * <tr><td style="border-style:solid; border-width:1px;">Methods called by the Application:</td>
 * <td style="border-style:solid; border-width:1px;"><ul>
 * <li>void createFile(String, String, int)</li>
 * <li>void createDirectory(String)</li>
 * <li>void deleteFileMaster(String)</li>
 * <li>void deleteDirectory(String)</li>
 * <li>void append(String, int, int, byte[], boolean)</li>
 * <li>void atomicAppend(String, int, byte[], boolean)</li>
 * <li>void read(String, int, int, String)</li>
 * <li>void readCompletely(String, String)</li>
 * </ul></td></tr>
 * <tr><td style="border-style:solid; border-width:1px;">Methods called by the Client:</td>
 * <td style="border-style:solid; border-width:1px;"><ul>
 * <li>void setupClientHost(Integer)</li>
 * <li>void connectToMaster()</li>
 * <li>void connectToChunkserver(Integer)</li>
 * </ul></td></tr>
 * </table>
 */
public interface ClientInterface extends Remote {
	//functions called by master
	/**
	 * Attempts to establish the RMI connection
	 * @param i	Client ID
	 * @throws RemoteException
	 */
	public void setupClientHost(Integer i) throws RemoteException;
	/**
	 * Attempts to establish the RMI connection to Master
	 * @throws RemoteException
	 */
	public void connectToMaster() throws RemoteException;
	/**
	 * Attempts to establish the RMI connection to Chunkservers
	 * @param chunkservers	map of Chunkserver IDs
	 * @throws RemoteException
	 */
	public void setChunkservers(Map<Integer, ChunkserverInterface> chunkservers) throws RemoteException;
	/**
	 * Attempts to establish the RMI connection to a Chunkserver
	 * @param id	Chunkserver ID
	 * @throws RemoteException
	 */
	public void connectToChunkserver(Integer id) throws RemoteException;
	/**
	 * Returns status
	 * @param requestType
	 * @param fullPath	full path of file
	 * @param succeeded
	 * @param ID
	 * @throws RemoteException
	 */
	public void requestStatus(String requestType, String fullPath, boolean succeeded, int ID) throws RemoteException;
	/**
	 * Asks Master for data 
	 * @param chunkhandle	full path of file
	 * @param ID
	 * @param chunkServersNum
	 * @param reqID
	 * @throws RemoteException
	 */
	public void passMetaData(String chunkhandle, int ID, List<Integer> chunkServersNum, int reqID) throws RemoteException;//chunkhandle, int id of primary, list of chunkservers

	// functions called by application
	/**
	 * Creates file
	 * @param Path	full path of file
	 * @param fileName
	 * @param numReplicas
	 * @throws RemoteException
	 */
	public void createFile(String Path, String fileName, int numReplicas)throws RemoteException;
	/**
	 * Creates directory
	 * @param path	full path of directory
	 * @throws RemoteException
	 */
	public void createDirectory(String path) throws RemoteException;
	/**
	 * Deletes file
	 * @param chunkhandle	full path of file
	 * @throws RemoteException
	 */
	public void deleteFileMaster(String chunkhandle) throws RemoteException;
	/**
	 * Deletes directory
	 * @param path	full path of directory
	 * @throws RemoteException
	 */
	public void deleteDirectory(String path) throws RemoteException;
	/**
	 * Appends data to file
	 * @param chunkhandle	full path of file
	 * @param offset	offset at which to append
	 * @param length	length of file
	 * @param data	data to be appended
	 * @param withSize	if true - expect byte array with haystack format; if false - byte array is only payload
	 * @throws RemoteException
	 */
	public void append(String chunkhandle, int offset, int length, byte[] data, boolean withSize) throws RemoteException;
	/**
	 * Appends data to end of file
	 * @param chunkhandle	full path of file
	 * @param length	length of file
	 * @param data	data to be appended
	 * @param withSize	if true - expect byte array with haystack format; if false - byte array is only payload
	 * @throws RemoteException
	 */
	public void atomicAppend(String chunkhandle, int length, byte[] data, boolean withSize) throws RemoteException;
	/**
	 * Read specified file
	 * @param chunkhandle	full path of file
	 * @param offset	offset at which to read in file
	 * @param length	length of file
	 * @param destination	file destination for read
	 * @throws RemoteException
	 */
	public void read(String chunkhandle, int offset, int length, String destination) throws RemoteException;
	/**
	 * Read specified file completely
	 * @param chunkhandle	full path of file
	 * @param destination
	 * @throws RemoteException
	 */
	public void readCompletely(String chunkhandle, String destination) throws RemoteException;
}
