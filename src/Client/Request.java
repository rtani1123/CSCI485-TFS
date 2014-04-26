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
	String requestType;
	String fullPath;
	String destination;
	int reqID;
	int primaryID;		// ID of chunkserver with primary lease
	enum rState{SentToMaster, ReceivedLocations, SentToChunk, Completed}
	rState state;
	List<Integer> chunkservers;
	byte[] payload;
	int length;
	int offset;
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
	 * Request constructor, used in the read and read completely methods
	 * in Client when it already has the location of the requested 
	 * chunkservers stored in its metadata 
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
	 * Make a deep copy of a list of chunkserver IDs and store
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
	 * Create a shallow copy of the data byte array
	 * @param data	byte array of data
	 */
	public void setData (byte [] data){
		/*payload = new byte[data.length];
		for(int i = 0; i < data.length; i++){
			payload[i] = data[i];
		}*/
		payload = data;
	}

	public void setLength(int length) {
		this.length = length;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public void setWithSize(boolean withSize) {
		this.withSize = withSize;
	}

	public boolean isReceived() {
		if (state == rState.ReceivedLocations) return true;
		else return false;
	}

	public boolean isSentToChunk() {
		if (state == rState.SentToChunk) return true;
		else return false;
	}
	public boolean isComplete() {
		if (state == rState.Completed) return true;
		else return false;
	}

	public void setReceived() {
		state = rState.ReceivedLocations;
	}

	public void setDestination (String d) {
		destination = d;
	}

	public void setPrimaryID(int primaryID) {
		this.primaryID = primaryID;
	}
	
	// getters
	public String getFullPath() {
		return fullPath;
	}
	public int getPrimaryID() {
		return primaryID;
	}
	public int getReqID() {
		return reqID;
	}
	public int getLength() {
		return length;
	}
	public int getOffset() {
		return offset;
	}
	public String getRequestType() {
		return requestType;
	}
	public byte[] getPayload() {
		return payload;
	}
	public List<Integer> getChunkservers() {
		return chunkservers;
	}
	public boolean getWithSize(){
		return withSize;
	}
}
