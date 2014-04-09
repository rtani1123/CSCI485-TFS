package Client;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class Client {

	Socket s;
	int masterClientPort = 55501; //DIFFERENT FROM MASTER/CHUNKSERVER PORT
	
	ObjectInputStream input;
	ObjectOutputStream output;
	
	public Client() {
		connectToMaster();
	}
	public boolean deleteFileClient(String filePath, String fileName){
		String deleteFileMsg = "$"+filePath+"$/"+fileName;
		connectToMaster();
		// connectToMaster should receive a message to pass to the master
		return false;
		
	}
	public void connectToMaster() {
		try {
			s = new Socket("dblab-05.vlab.usc.edu", masterClientPort);
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
	}
	public static void main(String[] args) {
		Client client = new Client();

	}

	class HandleMasterInput implements Runnable {
		Socket mySocket;
		
		HandleMasterInput(Socket s) {
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
