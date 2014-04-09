package Master;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

class AcceptChunkserverHandler implements Runnable {
		ServerSocket getClientsSS;
		Socket s;
		
		ObjectOutputStream output;
		ObjectInputStream input;
		
		Master parent;
		
		AcceptChunkserverHandler(Master parent, ServerSocket getClientsSS) {
			this.parent = parent;
			this.getClientsSS = getClientsSS;
			
			getConnection();
			
		}
		public void getConnection() {
			Socket s;
			try {
				s = getClientsSS.accept();
				output = new ObjectOutputStream(s.getOutputStream());
				input = new ObjectInputStream(s.getInputStream());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}  //waits for client protocol to connect
			//chunkservers.add(s);  //list of master's chunkservers
			
		}
		public void run() {
			while(true) {
				try {
					sendClientPort(s);
					//createClientThread(s);	//connect input/output streams
				} catch(Exception e) {
					System.out.println("Socket wasn't able to add");
					e.printStackTrace();
				}
			}
		} //end run
		
		private void sendClientPort(Socket s) {
			//sends the chunkserver new port to connect to master
			try {
				
				output.writeObject(new String("port"));
				
				if(receiveString().equals("accept port")) {
					Integer i;
					if(parent.chunkservers.isEmpty()) {
						i = new Integer(55501);
						parent.setupCSMasterDataConnection(i, s);
						output.writeObject(i);
					}
					else {
						i = parent.chunkservers.get((parent.chunkservers.size()-1)).getPort() +1;
						parent.setupCSMasterDataConnection(i, s);
						output.writeObject(new Integer(i));
						
					}
					System.out.println("Sent new chunkserver port " + i);
					
					
				}
			}
			catch(Exception e) {
				e.printStackTrace();
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
		} //end createClientThread
		
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
		
	} //end class ClientThreadHandler