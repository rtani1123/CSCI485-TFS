package Client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The Request class is used by the Client class to store information about a given request from the Application.
 * An instance of this class is created when the Client is told to append, atomic append, or read.
 * 
 *
 */

public class Request {
	String requestType; //Type is APPEND, ATOMIC APPEND, READ, etc..
	String fullPath; //chunkhandle
	String destination; //destination of READ location if necessary
	int reqID; //unique ID of request
	int primaryID;		// ID of chunkserver with primary lease
	enum rState{SentToMaster, ReceivedLocations, SentToChunk, Completed}
	rState state;
	List<Integer> chunkservers; //list of available chunkservers
	byte[] payload; //data to be appended if necessary
	int length; //length of data 
	int offset; //offset at which to append or read
	boolean withSize;

	/**
	 * Request constructor
	 * @param rq	request type (read)
	 * @param fp	full path or chunkhandle
	 * @param rID	request ID
	 */
	public Request(String rq, String fp, int rID) {
		requestType = rq;
		fullPath = fp;
		reqID = rID;
		state = rState.SentToMaster;
		chunkservers = Collections.synchronizedList(new ArrayList<Integer>());
	}

	/**
	 * Used in the read and read completely methods in Client when it already has the 
	 * location of the requested chunkservers stored in its metadata 
	 * @param rq	request type (read)
	 * @param fp	full path or chunkhandle
	 * @param rID	request ID
	 * @param cs	list of chunkserver replicas
	 */
	public Request(String rq, String fp, int rID, List<Integer> cs) {
		requestType = rq;
		fullPath = fp;
		reqID = rID;
		state = rState.ReceivedLocations;
		chunkservers = Collections.synchronizedList(new ArrayList<Integer>());
		for (int i = 0; i < cs.size(); i++) {
			chunkservers.add(cs.get(i));
		}
	}

	/**
	 * Makes a deep copy of a list of chunkserver IDs and stores them
	 * @param chunkserversList	list of replicas
	 */
	public void setCS (List<Integer> chunkserversList) {
		System.out.println("before setCS in request " +chunkservers.toString());
		// empty list of chunkservers if not empty
		chunkservers.clear();
		System.out.println("after clear setCS in request " +chunkservers.toString());
		if(chunkservers.size() != 0){
			for(int i = 0; i < chunkservers.size(); i++){
				chunkservers.remove(i);
			}
		}
		// deep copy into list of chunkservers
		for (int i = 0; i < chunkserversList.size(); i++) {
			chunkservers.add(chunkserversList.get(i));
		}
		System.out.println("After setCS in request: "+chunkservers.toString());
	}

	/**
	 * Creates a shallow copy of the data byte array
	 * @param data	byte array of data
	 */
	public void setData (byte [] data){
		/*payload = new byte[data.length];
		for(int i = 0; i < data.length; i++){
			payload[i] = data[i];
		}*/
		payload = data;
	}

	/**
	 * Sets the length of a read or append
	 * @param length
	 */
	public void setLength(int length) {
		this.length = length;
	}

	/**
	 * Sets the number of bytes to be offset for a read or append
	 * @param offset
	 */
	public void setOffset(int offset) {
		this.offset = offset;
	}

	/**
	 * Sets a boolean to control append and atomic append function in chunkserver
	 * If true, chunkservers expect to append an array of bytes starting with the size of the payload
	 * If false, chunkservers expect to append an array of bytes comprised of only the payload
	 * @param withSize
	 */
	public void setWithSize(boolean withSize) {
		this.withSize = withSize;
	}

	/**
	 * 
	 * @return true if request has received locations of task destinations.
	 */
	public boolean isReceived() {
		if (state == rState.ReceivedLocations) return true;
		else return false;
	}

	/**
	 * 
	 * @return true if sent to chunkserver.
	 */
	public boolean isSentToChunk() {
		if (state == rState.SentToChunk) return true;
		else return false;
	}
	
	/**
	 * 
	 * @return if request is completed.
	 */
	public boolean isComplete() {
		if (state == rState.Completed) return true;
		else return false;
	}

	/**
	 * 
	 */
	public void setReceived() {
		state = rState.ReceivedLocations;
	}

	/**
	 * 
	 * @param d
	 */
	public void setDestination (String d) {
		destination = d;
	}

	/**
	 * 
	 * @param primaryID
	 */
	public void setPrimaryID(int primaryID) {
		this.primaryID = primaryID;
	}
	
	// getters
	/**
	 * 
	 * @return full path of destination.
	 */
	public String getFullPath() {
		return fullPath;
	}
	
	/**
	 * 
	 * @return primary chunkserver ID.
	 */
	public int getPrimaryID() {
		return primaryID;
	}
	
	/**
	 * 
	 * @return request ID.
	 */
	public int getReqID() {
		return reqID;
	}
	
	/**
	 * 
	 * @return length of file in request.
	 */
	public int getLength() {
		return length;
	}
	
	/**
	 * 
	 * @return Integer offset.
	 */
	public int getOffset() {
		return offset;
	}
	
	/**
	 * 
	 * @return request type.
	 */
	public String getRequestType() {
		return requestType;
	}
	
	/**
	 * 
	 * @return payload
	 */
	public byte[] getPayload() {
		return payload;
	}
	
	/**
	 * 
	 * @return list of chunkservers
	 */
	public List<Integer> getChunkservers() {
		return chunkservers;
	}
	
	/**
	 * 
	 * @return withSize.
	 */
	public boolean getWithSize(){
		return withSize;
	}
}
