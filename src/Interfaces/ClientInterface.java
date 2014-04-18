package Interfaces;

import java.rmi.*;

public interface ClientInterface extends Remote {
	//functions called by master
	public void requestStatus(String requestType, String fullPath, boolean succeeded) throws RemoteException;
}
