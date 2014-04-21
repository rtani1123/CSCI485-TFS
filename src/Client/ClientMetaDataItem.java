package Client;

import java.util.ArrayList;

public class ClientMetaDataItem {
	String chunkhandle;
	int ID;
	ArrayList<Integer> chunkservers;
	
	public ClientMetaDataItem(String _chunkhandle, int _ID, ArrayList<Integer> _chunkservers){
		chunkhandle = _chunkhandle;
		ID = _ID;
		chunkservers = _chunkservers;
	}
}
