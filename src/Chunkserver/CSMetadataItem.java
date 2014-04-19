package Chunkserver;

public class CSMetadataItem {
	long startTime;
	int type;
	int stage; 
	String chunkhandle;
	/**
	 * 
	 * @param _startTime = Time stamp of the action
	 * @param _type = 0 for delete, 1 for create, 2 for append(update)
	 * @param _stage = 0 for received, 1 for commit
	 * @param _chunkhandle
	 */
	public CSMetadataItem(long _startTime, int _type, int _stage, String _chunkhandle){
		startTime = _startTime;
		type = _type;
		stage = _stage;
		chunkhandle = _chunkhandle;
	}
}
