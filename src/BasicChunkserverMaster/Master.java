package BasicChunkserverMaster;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import javax.swing.JOptionPane;

public class Master {

	/***** PORTS *********/
	final int incomingChunkserverPort = 45454;
	
	
	ServerSocket incomingChunkserverSS;
	ArrayList<Socket> chunkservers;
	
	public Master() {
		chunkservers = new ArrayList<Socket>();
		
		createServerSocket();
		
		//expect incoming chunkservers.
		new Thread(new HandleIncomingChunkservers(incomingChunkserverSS)).start();
	}
	
	public void createServerSocket() {
		try {
			incomingChunkserverSS = new ServerSocket(incomingChunkserverPort);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String args[]) {
		Master master = new Master();
	}
	class HandleIncomingChunkservers implements Runnable {
		ServerSocket ss;
		int port;
		
		HandleIncomingChunkservers(ServerSocket ss) {
			this.ss = ss;
		}
		public void run() {
			while(true) {
				try {
					Socket tempSocket = ss.accept();
					chunkservers.add(tempSocket);
					HandleChunkserverInput hci = new HandleChunkserverInput(tempSocket);
					new Thread(hci).start();
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	class HandleChunkserverInput implements Runnable {
		Socket socket;
		
		ObjectOutputStream output;
		ObjectInputStream input;
		
		HandleChunkserverInput(Socket socket) {
			this.socket = socket;
			try {
				output = new ObjectOutputStream(socket.getOutputStream());
				input = new ObjectInputStream(socket.getInputStream());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				JOptionPane.showMessageDialog(null, "Streams unable to connect to socket");
				e.printStackTrace();
			}
		}
		public void run() {
			String message;
			while(true) {
				try {
					message = receiveString(input);
					System.out.println("message = " + message);
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		private String receiveString(ObjectInputStream input) {
			Object obj = null;
			try {
				while ((obj = input.readObject()) != null) {
					if (obj instanceof String) {
						return ((String) obj);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.out.println("Unable to retrieve string info");
			}
			return "error";
		}
		
		public int receiveInt() {
			Object obj = null;
			try {
				while ((obj = input.readObject()) != null) {
					if (obj instanceof Integer) {
						return (Integer) obj;
					}
				}
			} catch (Exception e) {
				System.out.println("Unable to retrieve int info");
			}
			return 0;
		}//end receive int
	}
	class HandleChunkserverOutput implements Runnable {
		public void run() {
		}
	}
	
}
