package Client;

import java.util.ArrayList;

public class Request {
	String RequestType;
	String FullPath;
	int ID;
	enum rState{SentToMaster, ReceivedLocations, SentToChunk, Completed}
	rState state;
	ArrayList<Integer> chunkservers;

	
	public Request(String _rq, String _fp, int _id) {
		RequestType = _rq;
		FullPath = _fp;
		ID = _id;
		state = rState.SentToMaster;
	}
	
	public Request(String _rq, String _fp, int _id, ArrayList<Integer> cs) {
		RequestType = _rq;
		FullPath = _fp;
		ID = _id;
		state = rState.ReceivedLocations;
		for (int i = 0; i < cs.size(); i++) {
			int z = (int) cs.get(i);
			chunkservers.add(z);
		}
	}
	
	public void setCS (ArrayList<Integer> cs) {
		for (int i = 0; i < cs.size(); i++) {
			int z = (int) cs.get(i);
			chunkservers.add(z);
		}
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
}
