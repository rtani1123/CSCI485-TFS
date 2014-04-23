package Client;

import java.util.ArrayList;

public class ClientMetaDataItem {
	String chunkhandle;
	int ID;
	ArrayList<Integer> chunkservers;
	
	public ClientMetaDataItem(String _chunkhandle, int _ID, ArrayList<Integer> _chunkservers){
		chunkhandle = _chunkhandle;
		ID = _ID;
		chunkservers = new ArrayList<Integer>();
		for (int i = 0; i <  _chunkservers.size(); i++) {
			int z = (int)  _chunkservers.get(i);
			chunkservers.add(z);
		}
	}
	
	public void setID(int id) {
		ID = id;
	}
}
