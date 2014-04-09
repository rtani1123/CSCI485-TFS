package Master;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

class AcceptChunkserverHandler implements Runnable {
		ServerSocket getClientsSS;
		Socket s;
		
		ObjectOutputStream output;
		ObjectInputStream input;
		
		Master parent;
		
		AcceptChunkserverHandler(Master parent, ServerSocket getClientsSS, ObjectOutputStream output, ObjectInputStream input) {
			this.parent = parent;
			this.getClientsSS = getClientsSS;
			this.output = output;
			this.input = input;
			
		}
		public void run() {
			while(true) {
				try {
					Socket s = getClientsSS.accept();  //waits for client protocol to connect
					//chunkservers.add(s);  //list of master's chunkservers
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
				output = new ObjectOutputStream(s.getOutputStream());
				input = new ObjectInputStream(s.getInputStream());
				output.writeObject(new String("port"));
				
				if(receiveString().equals("accept port")) {
					Integer i;
					if(parent.chunkservers.isEmpty()) {
						i = new Integer(55501);
						parent.setupCSMasterDataConnection(i);
						output.writeObject(i);
					}
					else {
						i = parent.chunkservers.get((parent.chunkservers.size()-1)).getPort() +1;
						parent.setupCSMasterDataConnection(i);
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