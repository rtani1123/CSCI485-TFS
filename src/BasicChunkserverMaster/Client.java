package BasicChunkserverMaster;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

public class Client {
	Socket masterSocket;
	ObjectOutputStream output;
	ObjectInputStream input;
	public Client() {
		connectToMaster();
	}
	
	public void connectToMaster() {
		try {
			masterSocket = new Socket("localhost", 56565);
			output = new ObjectOutputStream(masterSocket.getOutputStream());
			input = new ObjectInputStream(masterSocket.getInputStream());
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		HandleServerInput hsi = new HandleServerInput(masterSocket, output, input);
		new Thread(hsi).start();
		
		try {
			//byte[] test = {1,2};
			//output.write(test);
			output.writeObject(new String("I am a client"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void main(String args[]) {
		Client cs = new Client();
	}
	
	class HandleServerInput implements Runnable {

		Socket mySocket;
		ObjectOutputStream output;
		ObjectInputStream input;
		
		public HandleServerInput(Socket s, ObjectOutputStream output, ObjectInputStream input) {
			mySocket = s;
			this.output = output;
			this.input = input;
		}
		
		public void run() {
			String message;
			
			while(true) {
				try {
					message = receiveString(input);
					System.out.println("message is: " + message);
				}
				catch(Exception e) {
					e.printStackTrace();
					System.out.println("Problem receiving message");
				}
			}//end while
		}//end run
		
		public String receiveString(ObjectInputStream input) {
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