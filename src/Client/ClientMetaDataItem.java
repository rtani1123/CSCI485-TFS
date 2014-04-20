package Client;

public class ClientMetaDataItem {
	long timeStamp;
	int type;
	boolean succeeded; 
	String chunkhandle;
	
	public ClientMetaDataItem(long _timeStamp, int _type, boolean _succeeded, String _chunkhandle){
		timeStamp = _timeStamp;
		type = _type;
		succeeded = _succeeded;
		chunkhandle = _chunkhandle;
	}
}
