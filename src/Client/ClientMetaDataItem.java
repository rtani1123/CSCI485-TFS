package Client;

import java.util.ArrayList;

/**
 * The ClientMetaDataItem class stores the replica chunkserver IDs and ID
 * of the primary chunkserver for a given chunkhandle (full path).  
 * It can be used by the Client in place of contacting the Master for read requests.
 */

public class ClientMetaDataItem {
	String chunkhandle;
	int ID;
	ArrayList<Integer> chunkservers;
	
	/**
	 * ClientMetaDataItem constructor
	 * @param _chunkhandle	full path or chunkhandle
	 * @param _ID			ID of chunkserver with the primary lease
	 * @param _chunkservers	list of chunkserver replicas
	 */
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
	
	// getters
	public ArrayList<Integer> getChunkservers() {
		return chunkservers;
	}
	
	public String getChunkhandle() {
		return chunkhandle;
	}
	
	public int getID() {
		return ID;
	}
}
