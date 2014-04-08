package Master;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class Master {

	ServerSocket ss;
	int clientMasterPort = 46344;
	ArrayList<Socket> chunkServers;
	
	ObjectOutputStream output;
	ObjectInputStream input;
	
	ClientThreadHandler cth;
	
	public Master() {
		chunkServers = new ArrayList<Socket>(); //initially empty list of at-some-point-connected chunkservers.
		setupServer();
		
	}
	
	public void setupServer() {
		try {
			ss = new ServerSocket(clientMasterPort);	//establish ServerSocket
		} catch (Exception e) {
			System.out.println("Port unavailable");
			e.printStackTrace();
			System.exit(0);
		}
		
		System.out.println("Server Created");
		cth = new ClientThreadHandler();	//new Thread to handle new or rebooting chunkservers
		new Thread(cth).start();
		
	}
	public static void main(String[] args) {
		Master master = new Master();
	}
	
	class ClientThreadHandler implements Runnable {
		
		public void run() {
			while(true) {
				try {
					Socket s = ss.accept();  //waits for client protocol to connect
					chunkServers.add(s);  //list of master's chunkservers
					createClientThread(s);	//connect input/output streams
				} catch(Exception e) {
					System.out.println("Socket wasn't able to add");
					e.printStackTrace();
				}
			}
		}

		private void createClientThread(Socket s) {
			ObjectOutputStream out = null;
			ObjectInputStream in = null;
			try {
				out = new ObjectOutputStream(s.getOutputStream());
				in = new ObjectInputStream(s.getInputStream());
			} catch (Exception e) {
				System.out.println("Streams unable to connect to socket");
				e.printStackTrace();
				System.exit(0);
			}
		}
	}

}
