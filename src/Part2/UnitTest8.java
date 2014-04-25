package Part2;

import java.rmi.RemoteException;

import Client.Client;

/*
 * Unit8:  Multiple instances of Test6 running using different TFS Clients, 
 * appending different images to one TFS file.
 * This requires atomic append where the TFS defines the offset at which it appends an image.
 */
public class UnitTest8 {
	public static Client client1 = null;
	public static Client client2 = null;
	public static void unitTest8Func(String chunkhandle) {
		try {
			client1 = new Client(11);
			client2 = new Client(12);
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		client1.atomicAppend(src, dest);
		
	}
}
