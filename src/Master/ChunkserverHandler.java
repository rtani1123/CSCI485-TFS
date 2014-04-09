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
	
	public ChunkserverHandler(Master parent, ObjectOutputStream output, ObjectInputStream input) {
		this.parent = parent;
		this.output = output;
		this.input = input;
	}
	public void run() {
		
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
