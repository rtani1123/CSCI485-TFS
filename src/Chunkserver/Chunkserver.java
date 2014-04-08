package Chunkserver;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class Chunkserver {
	
	Socket s;
	int portNumber = 46344;
	
	ObjectInputStream input;
	ObjectOutputStream output;
	
	public Chunkserver() {
		connectToMaster();
	}
	
	public void connectToMaster() {
		try {
			s = new Socket("dblab-18.vlab.usc.edu", portNumber);
		} catch(Exception e) {
			System.out.println("failure");
			e.printStackTrace();
			System.exit(0);
		}
		try {
			// Create the 2 streams for talking to the server
			output = new ObjectOutputStream(s.getOutputStream());
			input = new ObjectInputStream(s.getInputStream());
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		
		HandleServerInput hsin = new HandleServerInput(s);
		new Thread(hsin).start();
	}
	
	public static void main(String[] args) {
		Chunkserver cs = new Chunkserver();

	}

	class HandleServerInput implements Runnable {
		Socket mySocket;
		
		HandleServerInput(Socket s) {
			mySocket = s;
		}
		public void run() {
			String message;
			while(true) {
				try {
					message = receiveString();
					System.out.println("message is: " + message);
				}
				catch(Exception e) {
					e.printStackTrace();
					System.out.println("Problem receiving message");
				}
			}//end while
		}
		
		public String receiveString() {
			Object obj = null;
			try {
				while((obj = input.readObject()) != null) {
					if(obj instanceof String) {
						return ((String) obj);
					}
				}
			} catch(Exception e) {
				System.out.println("Unable to retrieve string info");
				e.printStackTrace();
			}
			return "error";
		}
	}
}
