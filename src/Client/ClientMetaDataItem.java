package Client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The ClientMetaDataItem class stores the replica chunkserver IDs and ID
 * of the primary chunkserver for a given chunkhandle (full path).  
 * It can be used by the Client in place of contacting the Master for read requests.
 */

public class ClientMetaDataItem {
	String chunkhandle;
	int ID;
	List<Integer> chunkservers;
	
	/**
	 * ClientMetaDataItem constructor
	 * @param _chunkhandle	full path or chunkhandle
	 * @param _ID			ID of chunkserver with the primary lease
	 * @param chunkserversList	list of chunkserver replicas
	 */
	public ClientMetaDataItem(String _chunkhandle, int _ID, List<Integer> chunkserversList){
		chunkhandle = _chunkhandle;
		ID = _ID;
		chunkservers = Collections.synchronizedList(new ArrayList<Integer>());
		for (int i = 0; i <  chunkserversList.size(); i++) {
			int z = (int)  chunkserversList.get(i);
			chunkservers.add(z);
		}
	}
	
	public void setID(int id) {
		ID = id;
	}
	
	public void setChunkservers(List<Integer> chunkserversList) {
		for (int i = 0; i < chunkservers.size(); i++) {
			chunkservers.remove(0);
		}
		for (int i = 0; i <  chunkserversList.size(); i++) {
			int z = (int)  chunkserversList.get(i);
			chunkservers.add(z);
		}
	}
	
	// getters
	public List<Integer> getChunkservers() {
		return chunkservers;
	}
	
	public String getChunkhandle() {
		return chunkhandle;
	}
	
	public int getID() {
		return ID;
	}
}
