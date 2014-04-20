package Interfaces;

import java.rmi.*;
import java.util.ArrayList;

public interface ClientInterface extends Remote {
	//functions called by master
	public void requestStatus(String requestType, String fullPath, boolean succeeded, int ID) throws RemoteException;
	public void passMetaData(int chunkhandle, int ID, ArrayList<Integer> chunkservers);//chunkhandle, int id of primary, list of chunkservers
}
