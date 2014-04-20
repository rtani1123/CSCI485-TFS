package Client;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.rmi.RemoteException;
import java.util.ArrayList;

import Interfaces.ClientInterface;

public class Client implements ClientInterface{
	ArrayList<ClientMetaDataItem> clientMetaDataArray;
	
	public Client() {
		clientMetaDataArray = new ArrayList<ClientMetaDataItem>();	
	}


	public static void main(String[] args) {
		Client client = new Client();

	}


	public void requestStatus(String requestType, String fullPath, boolean succeeded, int ID) throws RemoteException {
		
		if (succeeded) {
			System.out.println("Request to" + requestType + " " + fullPath + "succeeded.");
		}
		else {
			System.out.println("Request to" + requestType + " " + fullPath + "failed.");
		}
		
	}
	


	@Override


	public void passMetaData(String chunkhandle, int ID,
			ArrayList<Integer> chunkservers) {
		// TODO Auto-generated method stub

		
	}

	
}
