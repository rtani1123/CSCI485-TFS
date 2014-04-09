package BasicChunkserverMaster;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

public class Chunkserver {
	
	Socket masterSocket;
	ObjectOutputStream output;
	ObjectInputStream input;
	public Chunkserver() {
		connectToMaster();
	}
	
	public void connectToMaster() {
		try {
			masterSocket = new Socket("localhost", 45454);
			output = new ObjectOutputStream(masterSocket.getOutputStream());
			input = new ObjectInputStream(masterSocket.getInputStream());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		HandleServerInput hsi = new HandleServerInput(masterSocket);
		new Thread(hsi).start();
		
		try {
			output.writeObject(new String("he"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void main(String args[]) {
		Chunkserver cs = new Chunkserver();
	}
	
	class HandleServerInput implements Runnable {

		Socket mySocket;
		
		public HandleServerInput(Socket s) {
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
		}//end run
		
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
		
	}//end HandleServerInput class
}
