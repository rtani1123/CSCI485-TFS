package Client;

import java.util.ArrayList;

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
	int ID;		// ID of chunkserver with primary lease
	enum rState{SentToMaster, ReceivedLocations, SentToChunk, Completed}
	rState state;
	ArrayList<Integer> chunkservers;
	byte[] payload;
	int length;
	int offset;
	boolean withSize;
	
	/**
	 * Request constructor
	 * @param _rq	request type (append, atomic append, or read)
	 * @param _fp	full path or chunkhandle
	 * @param _id	ID of chunkserver with the primary lease
	 */
	
	public Request(String _rq, String _fp, int _id) {
		requestType = _rq;
		fullPath = _fp;
		ID = _id;
		state = rState.SentToMaster;
		chunkservers = new ArrayList<Integer>();
	}
	
	/**
	 * Request constructor, used in the read method in Client when it
	 * already has the location of the requested chunkservers stored 
	 * in its metadata 
	 * @param _rq	request type (read)
	 * @param _fp	full path or chunkhandle
	 * @param _id	ID of chunkserver with the primary lease
	 * @param cs	list of replicas
	 */
	public Request(String _rq, String _fp, int _id, ArrayList<Integer> cs) {
		requestType = _rq;
		fullPath = _fp;
		ID = _id;
		state = rState.ReceivedLocations;
		chunkservers = new ArrayList<Integer>();
		for (int i = 0; i < cs.size(); i++) {
			chunkservers.add(cs.get(i));
		}
	}
	
	/**
	 * Make a deep copy of a list of chunkserver IDs and store
	 * @param cs	list of replicas
	 */
	public void setCS (ArrayList<Integer> cs) {
		// empty list of chunkservers if not empty
		if(chunkservers.size() != 0){
			for(int i = 0; i < chunkservers.size(); i++){
				chunkservers.remove(i);
			}
		}
		// deep copy into list of chunkservers
		for (int i = 0; i < cs.size(); i++) {
			chunkservers.add(cs.get(i));
		}
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
	
	
	// getters
	public String getFullPath() {
		return fullPath;
	}
	public int getID() {
		return ID;
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
	public ArrayList<Integer> getChunkservers() {
		return chunkservers;
	}
	public boolean getWithSize(){
		return withSize;
	}
}
