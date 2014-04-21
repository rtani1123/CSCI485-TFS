package Chunkserver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

public class Chunkserver_OLD {

	Socket s;
	Socket connection;
	int initPort = 46344;
	int masterPort;
	ObjectInputStream input;
	ObjectOutputStream output;
	
	Thread initialConnection;
	Map<String,Metadata> files;

	public Chunkserver_OLD() {
		files = new HashMap<String,Metadata>();
		connectToMaster();
	}

	public void createFileHandshake(String pathName){
		File f = new File(pathName);
		StringBuffer message = new StringBuffer();
		try {
			if(f.createNewFile()){
				//message master to say file creation successful
				message.append("$exists$");
				message.append(pathName);
				message.append("$");
			}
			else{
				//message master to say file creation failed
				message.append("$failed$");
				message.append(pathName);
				message.append("$");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try{
			// **look up output stream for this port
			output.write(String.valueOf(message).getBytes());
		}catch(Exception e){
			
		}
		
		// store metadata
		files.put(pathName, new Metadata(pathName));

		// update directory structure
	}
	
	//**currently not checking for stale writes using timestamps
	public void append(String chunkhandle, int offset, int length, byte[] data){
		File f = new File(chunkhandle);	// might have to parse chunkhandle into path
		StringBuffer message = new StringBuffer();
		try {
			RandomAccessFile raf = new RandomAccessFile(f,"rws");
			raf.write(data, offset, length);
			// change write timestamp
			files.get(chunkhandle).setWriteTime(System.currentTimeMillis());
		} catch (Exception e) {
			e.printStackTrace();
		}
		// message success
		message.append("$append$");
		message.append(chunkhandle);
		message.append("$");
		message.append(files.get(chunkhandle).getWriteTime());
		message.append("$");
		
		try{
			// **look up output stream for this port
			output.write(String.valueOf(message).getBytes());
		}catch(Exception e){
			
		}
	}
	
	//**currently not checking for stale writes using timestamps
	public void atomicAppend(String chunkhandle, int length, byte[] data){
		File f = new File(chunkhandle);	// might have to parse chunkhandle into path
		StringBuffer message = new StringBuffer();
		try {
			RandomAccessFile raf = new RandomAccessFile(f,"rws");
			raf.seek(raf.length());
			// **before writing, this offset will need to be saved and sent to the secondaries
			// **it should be the getFilePointer()
			raf.write(data);
			// change write timestamp
			files.get(chunkhandle).setWriteTime(System.currentTimeMillis());
		} catch (Exception e) {
			e.printStackTrace();
		}
		// message success
		message.append("$append$");
		message.append(chunkhandle);
		message.append("$");
		message.append(files.get(chunkhandle).getWriteTime());
		message.append("$");
		
		try{
			// **look up output stream for this port
			output.write(String.valueOf(message).getBytes());
		}catch(Exception e){
			
		}
	}
		
	public void read(String chunkhandle, int offset, int length){
		File f = new File(chunkhandle);	// might have to parse chunkhandle into path
		StringBuffer message = new StringBuffer();
		byte[] b = new byte[length];
		try {
			RandomAccessFile raf = new RandomAccessFile(f,"r");
			raf.seek(raf.length());
			// **before writing, this offset will need to be saved and sent to the secondaries
			// **it should be the getFilePointer()
			raf.readFully(b,offset,length);
			// change write timestamp
			files.get(chunkhandle).setReadTime(System.currentTimeMillis());
		} catch (Exception e) {
			e.printStackTrace();
			// might want to send a message with some error
			return;
		}
		// message success
		message.append("$");
		message.append(chunkhandle);
		message.append("$");
		message.append(length);
		message.append("$");
		
		try{
			// **look up output stream for this port
			output.write(String.valueOf(message).getBytes());
			output.write(b);
		}catch(Exception e){
			
		}
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
		
		try {
			// Create the 2 streams for talking to the server
			output = new ObjectOutputStream(connection.getOutputStream());
			input = new ObjectInputStream(connection.getInputStream());
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
		System.out.println("Successfully created socket connection over master port " + masterPort + " and created I/O streams");
		HandleMasterInput hsin = new HandleMasterInput(connection);
		new Thread(hsin).start();
	}

	public static void main(String[] args) {
		Chunkserver_OLD cs = new Chunkserver_OLD();
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
					
					if(message.equals("port")) {
						output.writeObject(new String("accept port"));
						masterPort = receiveInt();
						System.out.println("Received port " + masterPort);
						establishMasterConnection(masterPort);
					}
						
				} catch (Exception e) {
					
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
	
	class Metadata{
		//filepath, read/write timestamps
		String filePath;
		long readTime;
		long writeTime;
		
		public Metadata(String filePath){
			this.filePath = filePath;
			readTime = System.currentTimeMillis();
			writeTime = System.currentTimeMillis();
		}
		
		// getters
		public String getFilePath() {
			return filePath;
		}
		public long getReadTime() {
			return readTime;
		}
		public long getWriteTime() {
			return writeTime;
		}
		
		// setters
		public void setFilePath(String filePath) {
			this.filePath = filePath;
		}
		public void setReadTime(long readTime) {
			this.readTime = readTime;
		}
		public void setWriteTime(long writeTime) {
			this.writeTime = writeTime;
		}
	}
}