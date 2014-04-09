package Chunkserver;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;

public class Chunkserver {

	Socket s;
	Socket connection;
	int initPort = 46344;
	int masterPort;
	ObjectInputStream input;
	ObjectOutputStream output;
	
	Thread initialConnection;

	public Chunkserver() {
		connectToMaster();
	}

		
	public static boolean deleteFileChunk(String path) {
		 String parsedPath = path.replace("$", "");
		try {
			 File dFile = new File(parsedPath);
			 System.out.println(parsedPath);
			 String[] files =null;
			 if(dFile.isDirectory())
				 files= dFile.list();
			 if (dFile.isFile() || (files.length==0))
				 dFile.delete();
			 else if (dFile.isDirectory()){
				 for (int i = 0; i < files.length; i++){
					 deleteFileChunk(parsedPath +"/"+files[i]);
				 }
				 dFile.delete();
			 }
				 
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	public static boolean runCMDCommands(String command) {

		try {

			ProcessBuilder builder = new ProcessBuilder("cmd.exe", command);
			builder.redirectErrorStream(true);
			Process p;
			p = builder.start();
//			BufferedReader r = new BufferedReader(new InputStreamReader(
//					p.getInputStream()));
//			String line;
//			while (true) {
//				line = r.readLine();
//				if (line == null) {
//					break;
//				}
//				System.out.println(line);
//			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;

	}

	public void connectToMaster() {
		try {
			s = new Socket("localhost", initPort);
		} catch (Exception e) {
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

		HandleMasterInput hsin = new HandleMasterInput(s);
		initialConnection = new Thread(hsin);
		initialConnection.start();
	}
	
	private void establishMasterConnection(int masterPort) {
		// TODO Auto-generated method stub
		this.masterPort = masterPort;
		try {
			s.close();
			connection = new Socket("localhost", masterPort);
		} catch (Exception e) {
			System.out.println("failure");
			e.printStackTrace();
			System.exit(0);
		}
		System.out.println("successfully connected");
		try {
			// Create the 2 streams for talking to the server
			output = null;
			input = null;
			output = new ObjectOutputStream(connection.getOutputStream());
			input = new ObjectInputStream(connection.getInputStream());
			initialConnection.destroy();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		
	}

	public static void main(String[] args) {
		Chunkserver cs = new Chunkserver();
		//deleteFileChunk("C:/Users/boghrati/Downloads/del");

	}

	class HandleMasterInput implements Runnable {
		Socket mySocket;

		HandleMasterInput(Socket s) {
			mySocket = s;
		}

		public void run() {
			String message;
			boolean run = true;
			while (run) {
				try {
					message = receiveString();
					System.out.println("message is: " + message);
					
					if(message.equals("port")) {
						output.writeObject(new String("accept port"));
						masterPort = receiveInt();
						System.out.println("Received port " + masterPort);
						establishMasterConnection(masterPort);
						run = false;
					}
						
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println("Problem receiving message");
				}
			}// end while
		}

		

		public String receiveString() {
			Object obj = null;
			try {
				while ((obj = input.readObject()) != null) {
					if (obj instanceof String) {
						return ((String) obj);
					}
				}
			} catch (Exception e) {
				System.out.println("Unable to retrieve string info");
				e.printStackTrace();
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
}
