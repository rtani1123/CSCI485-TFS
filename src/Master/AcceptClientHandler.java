package Master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class AcceptClientHandler implements Runnable{
	
	Master parent;
	Socket s;
	ServerSocket ss;
	
	ObjectOutputStream clientSetupOutput;
	ObjectInputStream clientSetupInput;
	
	public AcceptClientHandler(Master parent, ServerSocket ss) {
		this.parent = parent;
		this.ss = ss;
		getConnection();
	}
	
	public void getConnection() {
		try {
			s = ss.accept();
			clientSetupOutput = new ObjectOutputStream(s.getOutputStream());
			clientSetupInput = new ObjectInputStream(s.getInputStream());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void setSocket(Socket s) {
		this.s = s;
	}
	public void run() {
		while(true) {
			try {
				
			} catch(Exception e) {
				
			}
		}
	}
	public String receiveString() {
		Object obj = null;
		try {
			while((obj = clientSetupInput.readObject()) != null) {
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
