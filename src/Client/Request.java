package Client;

public class Request {
	String RequestType;
	String FullPath;
	int ID;
	enum rState{SentToMaster, ReceivedLocations, SentToChunk, Completed}

	
	public Request(String _rq, String _fp, int _id) {
		RequestType = _rq;
		FullPath = _fp;
		ID = _id;
	}
}
