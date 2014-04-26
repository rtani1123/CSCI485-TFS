package Chunkserver;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * The ChunkserverMetadata class is used by the Chunkservers to store relevant information about 
 * files they have created.  It is used to maintain a connection to secondaries for an atomic
 * append.  There is one instance of this class for each file stored in a Chunkserver.  It holds
 * write timestamps in addition to serving as a node in the Chunkserver's directory structure.
 * This class may be stored on the disk to facilitate Chunkserver restoration after a failure
 * and it provides static methods to read and write String-ChunkserverMetadata maps to disk.
 */
public class ChunkserverMetadata implements Serializable {
	long primaryLeaseTime;
	long writeTime;
	List<Integer> secondaries = Collections.synchronizedList(new ArrayList<Integer>());
	String chunkhandle;

	/**
	 * Constructor, stores the full path and the current write time
	 * @param chunkhandle	- full path for the file stored
	 */
	public ChunkserverMetadata(String chunkhandle){
		writeTime = System.currentTimeMillis();
		primaryLeaseTime = -1;
		this.chunkhandle = chunkhandle;
	}

	/**
	 * Updates the write timestamp for the file
	 * @param time	- new write timestamp
	 */
	public void setWriteTime(long time){
		this.writeTime = time;
	}

	/**
	 * Updates the primary lease timestamp for the file
	 * Primary leases expire after a set period of time specified in the Chunkserver
	 * @param time	- new primary lease timestamp
	 */
	public void setPrimaryLeaseTime(long time){
		primaryLeaseTime = time;
	}

	/**
	 * Updates the list of secondary Chunkserver IDs 
	 * @param secondaries	- integer list of secondary Chunkserver IDs
	 */
	public void setSecondaries(List<Integer> secondaries){
		this.secondaries = secondaries;
	}

	/**
	 * Returns the last write timestamp for the file
	 * @return The most recent write timestamp for the file
	 */
	public long getWriteTime(){
		return writeTime;
	}

	/**
	 * Returns the primary lease time
	 * @return	The time at which the primary lease was granted for an atomic append
	 */
	public long getPrimaryLeaseTime() {
		return primaryLeaseTime;
	}

	/**
	 * Returns the list of secondary Chunkserver IDs
	 * @return	The stored list of secondary Chunkserver IDs for the file
	 */
	public List<Integer> getSecondaries() {
		return secondaries;
	}

	/**
	 * Writes a map of chunkhandles to ChunkserverMetadata objects to disk
	 * @param cm	- map of file full paths to ChunkserverMetadata objects
	 */
	static public void storeTree(Map<String, ChunkserverMetadata> cm) {
		String filename = "csmetadata";
		FileOutputStream fos = null;
		ObjectOutputStream out = null;
		try {
			fos = new FileOutputStream(filename);
			out = new ObjectOutputStream(fos);
			out.writeObject(cm);
			out.close();
			System.out.println("Chunkhandle Metadata Persisted");
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
	
	/**
	 * Returns the full map of all file full paths to ChunkserverMetadata objects stored on the disk
	 * @return	Full map of all file full paths to ChunkserverMetadata objects
	 */
	static public Map<String, ChunkserverMetadata> getMetadata() {
		String filename = "csmetadata";
		Map<String, ChunkserverMetadata> cm = null;
		File f = new File(filename);
		if (!f.exists()) {
			System.out.println("No stored chunkserver metadata file exists.");
			return null;
		}
		FileInputStream fis = null;
		ObjectInputStream oin = null;
		try {
			if (f.length() > 0) {
				fis = new FileInputStream(filename);
				oin = new ObjectInputStream(fis);
				cm = (Map<String, ChunkserverMetadata>) oin.readObject();
				oin.close();
			}
		} catch (IOException ex) {
			System.out.println("Error in reading chunkserver metadata");
			ex.printStackTrace();
		} catch (ClassNotFoundException ex) {
			System.out.println("Error in reading chunkserver metadata");
			ex.printStackTrace();
		}
		return cm;
	}
}
