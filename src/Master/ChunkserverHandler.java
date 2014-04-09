package Master;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class ChunkserverHandler implements Runnable{

	Master parent;
	
	ServerSocket ss;
	Socket s;
	
	ObjectOutputStream output;
	ObjectInputStream input;
	
	public ChunkserverHandler(Master parent, ServerSocket server) {
		this.parent = parent;
		this.ss = server;
		
		getChunkserver();
	}
	
	public void getChunkserver() {
		
	}
	public void run() {
		while(true) {
			try {
				Socket s = ss.accept();  //waits for client protocol to connect
				output = new ObjectOutputStream(s.getOutputStream());
				input = new ObjectInputStream(s.getInputStream());
				System.out.println("hi");
				parent.output.writeObject(new String("test"));
				
			} catch(Exception e) {
				System.out.println("Socket wasn't able to add");
				e.printStackTrace();
				
			}
		}
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
