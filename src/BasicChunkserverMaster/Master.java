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
	//CHANGE INCOMING CHUNKSERVERS AND CLIENTS TO INCOMING CONNECTIONS
	/***** PORTS *********/
	final int incomingChunkserverPort = 45454;
	final int incomingClientPort = 56565;
	
	ServerSocket incomingChunkserverSS;
	ServerSocket incomingClientSS;
	
	ArrayList<MyClient> clients;
	ArrayList<MyChunkserver> chunkservers;
	
	public Master() {
		chunkservers = new ArrayList<MyChunkserver>();
		clients = new ArrayList<MyClient>();
		createServerSocket();
		
		//expect incoming chunkservers.
		new Thread(new HandleIncomingChunkservers(incomingChunkserverSS)).start();
		System.out.println("Accepting Chunkservers");
		new Thread(new HandleIncomingClients(incomingClientSS)).start();
		System.out.println("Accepting Clients");
	}
	
	public void createServerSocket() {
		try {
			incomingChunkserverSS = new ServerSocket(incomingChunkserverPort);
			incomingClientSS = new ServerSocket(incomingClientPort);
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
					MyChunkserver temp = new MyChunkserver(tempSocket);
					chunkservers.add(temp);
					HandleChunkserverInput hci = new HandleChunkserverInput(temp);
					HandleChunkserverOutput hco = new HandleChunkserverOutput(temp);
					new Thread(hci).start();
					new Thread(hco).start();
					System.out.println("Successfully connected Chunkserver");
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	class HandleIncomingClients implements Runnable {
		ServerSocket ss;
		
		HandleIncomingClients(ServerSocket ss) {
			this.ss = ss;
		}
		
		public void run() {
			while(true) {
				try {
					Socket tempSocket = ss.accept();
					MyClient temp = new MyClient(tempSocket);
					clients.add(temp);
					HandleClientInput hci = new HandleClientInput(temp);
					HandleClientOutput hco = new HandleClientOutput(temp);
					new Thread(hci).start();
					new Thread(hco).start();
					System.out.println("Successfully connected Client");
				} catch(Exception e) {
					System.out.println("Problem creating HandleIncomingClients");
					e.printStackTrace();
				}
			}
		}
	}
	
	class HandleChunkserverInput implements Runnable {
		MyChunkserver chunkserver;
		
		ObjectOutputStream output;
		ObjectInputStream input;
		
		HandleChunkserverInput(MyChunkserver myCS) {
			this.chunkserver = myCS;
			try {
				output = new ObjectOutputStream(myCS.getSocket().getOutputStream());
				input = new ObjectInputStream(myCS.getSocket().getInputStream());
				chunkserver.setOutput(output);
				chunkserver.setInput(input);
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
					System.out.println("chunkserver message = " + message);
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		
	}
	class HandleChunkserverOutput implements Runnable {
		MyChunkserver chunkserver;
		HandleChunkserverOutput(MyChunkserver myCS) {
			this.chunkserver = myCS;
		}
		public void run() {
		}
	}
	
	class HandleClientInput implements Runnable {
		MyClient client;
		
		ObjectOutputStream output;
		ObjectInputStream input;
		
		HandleClientInput(MyClient myClient) {
			this.client = myClient;
			try {
				output = new ObjectOutputStream(client.getSocket().getOutputStream());
				input = new ObjectInputStream(client.getSocket().getInputStream());
				client.setOutput(output);
				client.setInput(input);
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
					System.out.println("client message = " + message);
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	class HandleClientOutput implements Runnable {
		MyClient client;
		HandleClientOutput(MyClient myClient) {
			this.client = myClient;
		}
		public void run() {
		}
	}
	
	class MyChunkserver {
		Socket socket;
		ObjectOutputStream output;
		ObjectInputStream input;
		
		MyChunkserver(Socket s) {
			socket = s;
		}
		Socket getSocket() {
			return socket;
		}
		ObjectOutputStream getOutput() {
			return output;
		}
		ObjectInputStream getInput() {
			return input;
		}
		void setSocket(Socket socket) {
			this.socket = socket;
		}
		void setOutput(ObjectOutputStream output) {
			this.output = output;
		}
		void setInput(ObjectInputStream input) {
			this.input = input;
		}
	}
	class MyClient {
		Socket socket;
		ObjectOutputStream output;
		ObjectInputStream input;
		
		MyClient(Socket s) {
			socket = s;
		}
		Socket getSocket() {
			return socket;
		}
		ObjectOutputStream getOutput() {
			return output;
		}
		ObjectInputStream getInput() {
			return input;
		}
		void setSocket(Socket socket) {
			this.socket = socket;
		}
		void setOutput(ObjectOutputStream output) {
			this.output = output;
		}
		void setInput(ObjectInputStream input) {
			this.input = input;
		}
	}
	//receipt methods
	public String receiveString(ObjectInputStream input) {
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
	public int receiveInt(ObjectInputStream input) {
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
