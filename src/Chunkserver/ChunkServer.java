package Chunkserver;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.rmi.Naming;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import Interfaces.ChunkserverInterface;
import Interfaces.ClientInterface;
import Interfaces.MasterInterface;
import Utilities.Tree;

public class ChunkServer extends UnicastRemoteObject implements ChunkserverInterface {
	Map<String, Metadata> CSMetadata = new HashMap<String, Metadata>();
	Map<Integer, ChunkserverInterface> chunkservers  = new HashMap<Integer, ChunkserverInterface>();
	MasterInterface myMaster;
	ClientInterface myClient;
	static Integer csIndex;
	public static final long LEASETIME = 60000;

	public ChunkServer() throws RemoteException {
		//setupMasterChunkserverHost();
		//setupMasterChunkserverClient();
		chunkservers = new HashMap<Integer, ChunkserverInterface>();
		setupChunkserverHost();

		connectToMaster();
		myMaster.connectToChunkserver(csIndex);
		connectToClient();
		myClient.connectToChunkserver(csIndex);
	}

	//Master calls Chunkserver methods -> CHUNK + csIndex
	public void setupChunkserverHost() {
		try {
			System.setSecurityManager(new RMISecurityManager());
			if(csIndex == 1) {
				Registry registry = LocateRegistry.createRegistry(123);
				Naming.bind("rmi://dblab-36.vlab.usc.edu:123/CHUNK" + csIndex.toString(), this);
			}
			else if(csIndex == 2) {
				Registry registry = LocateRegistry.createRegistry(124);
				Naming.bind("rmi://dblab-05.vlab.usc.edu:124/CHUNK" + csIndex.toString(), this);
			}
			else if(csIndex == 3) {
				Registry registry = LocateRegistry.createRegistry(125);
				Naming.bind("rmi://dblab-29.vlab.usc.edu:125/CHUNK" + csIndex.toString(), this);
			}
			System.out.println("Chunkserver " + csIndex + " Host Setup Success");
		} catch (RemoteException e) {
			//index = index + 1;
			//setupChunkserverHost(index);
			e.printStackTrace();
		} catch (Exception e) {
			//e.printStackTrace();
		}
	}

	//Chunkserver calls Master Methods -> MASTER
	public void connectToMaster() {
		try {
			System.setSecurityManager(new RMISecurityManager());
			/*
			 * format for connection should be "rmi:DOMAIN/ChunkserverID".
			 * ChunkserverID will be different for each instance of Chunkserver
			 * one detail to mention is that CSMaster will not be the same as
			 * MasterCS. There's actually a completely different callfor master
			 * calling CS functions than CS calling master functions.
			 * 
			 * For this, the master is hosted on dblab-29.
			 */
			myMaster = (MasterInterface) Naming
					.lookup("rmi://dblab-18.vlab.usc.edu/MASTER");
			System.out.println("Connection to Master Success");
			/*
			 * ChunkServer FUNCTION HOST implementation
			 */

		} catch (Exception re) {
			re.printStackTrace();
		}
	}

	//Chunkserver calls Client Methods -> CLIENT
	public void connectToClient() {
		try {
			System.setSecurityManager(new RMISecurityManager());

			myClient = (ClientInterface)Naming.lookup("rmi://dblab-29.vlab.usc.edu/CLIENT");
			System.out.println("Connection to Client Success");

		} catch(Exception re) {
			re.printStackTrace();
		}
	}

	public void connectToChunkserver(Integer index) {
		try {
			System.setSecurityManager(new RMISecurityManager());
			ChunkserverInterface tempCS = null;
			System.out.println("Connected to " + index);
			if(index == 1)
				tempCS = (ChunkserverInterface)Naming.lookup("rmi://dblab-36.vlab.usc.edu:123/CHUNK" + index.toString());
			else if(index == 2)
				tempCS = (ChunkserverInterface)Naming.lookup("rmi://dblab-05.vlab.usc.edu:124/CHUNK" + index.toString());
			else if(index == 3)
				tempCS = (ChunkserverInterface)Naming.lookup("rmi://dblab-29.vlab.usc.edu:125/CHUNK" + index.toString());
			chunkservers.put(index, tempCS);
			tempCS.connectToChunkserver(csIndex);
			/*
			 * ChunkServer FUNCTION HOST implementation
			 */

		} catch(Exception re) {
			re.printStackTrace();
		}
	}
	@Override
	public Map<String, Long> refreshMetadata() throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see Interfaces.ChunkserverInterface#primaryLease(java.lang.String,
	 * java.util.ArrayList)
	 */
	@Override
	public void primaryLease(String chunkhandle, ArrayList<Integer> CServers)
			throws RemoteException {
		CSMetadata.get(chunkhandle).setPrimaryLeaseTime(System.currentTimeMillis());
		CSMetadata.get(chunkhandle).setSecondaries(CServers);
	}

	@Override
	public boolean createFile(String chunkhandle) throws RemoteException {
		File f = new File(chunkhandle);
		try {
			if (!f.createNewFile()) {
				System.err.println("File creation unsuccessful " + chunkhandle);
				return false;
			}
			Metadata md = new Metadata(chunkhandle);
			CSMetadata.put(chunkhandle, md);
		} catch (Exception e) {
			System.out.println("File creation unsuccessful");
			e.printStackTrace();
			return false;
		}
		return true;
	}

	@Override
	public boolean createDirectory(String chunkhandle) throws RemoteException {
		File f = new File(chunkhandle);
		try {
			if (!f.mkdir()) {
				System.err.println("Directory creation unsuccessful "
						+ chunkhandle);
				return false;
			}
		} catch (Exception e) {
			System.out.println("Directory creation unsuccessful");
			e.printStackTrace();
			return false;
		}
		return true;
	}

	@Override
	public boolean deleteFile(String chunkhandle) throws RemoteException {
		String parsedPath = chunkhandle;
		try {
			File dFile = new File(parsedPath);
			System.out.println("Now Deleting " + parsedPath);
			String[] files = null;
			if (dFile.isDirectory())
				files = dFile.list();
			if (dFile.isFile() || (files.length == 0)) {
				if (!dFile.delete()) {
					System.out.println("Delete file unsuccessful");
					return false;
				}
			} else if (dFile.isDirectory()) {
				for (int i = 0; i < files.length; i++) {
					deleteFile(parsedPath + "/" + files[i]);
				}
				if (!dFile.delete()) {
					System.out.println("Delete file unsuccessful");
					return false;
				}
			}

		} catch (Exception e) {
			System.out.println("Delete file unsuccessful");
			e.printStackTrace();
			return false;
		}
		return true;
	}

	@Override
	public boolean deleteDirectory(String chunkhandle) throws RemoteException {
		if (deleteFile(chunkhandle)) {

		} else {
			System.out.println("Delete unsuccessful. Item not found.");
			return false;
		}
		return true;
	}

	@Override
	public byte[] read(String chunkhandle, int offset, int length)
			throws RemoteException {
		File f = new File(chunkhandle); // might have to parse chunkhandle into
		// path

		byte[] b = new byte[length];
		try {
			RandomAccessFile raf = new RandomAccessFile(f, "r");
			raf.seek(offset);
			raf.readFully(b);
			raf.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.out
			.println("Error in creating RandomAccessFule in read. Returning byte array of 0 size.");
			return new byte[0];
		}
		return b;
	}

	@Override
	public boolean append(String chunkhandle, byte[] payload, int length,
			int offset, boolean withSize) throws RemoteException {
		File f = new File(chunkhandle);
		System.out.println("Chunkserver append was called.");
		try {
			RandomAccessFile raf = new RandomAccessFile(f, "rws");
			raf.seek(offset);
			System.err.println("Append offset by: " + offset);
			raf.write(payload);
			raf.close();
		} catch (IOException e) {
			System.out.println("Append was unsuccessful");
			e.printStackTrace();
			return false;
		}
		// TODO : add to metadata
		CSMetadata.get(chunkhandle).setWriteTime(System.currentTimeMillis());

		return true;
	}

	@Override
	public boolean atomicAppend(String chunkhandle, byte[] payload, int length,
			boolean withSize) throws RemoteException {
		/*
		 * If it's primary, it'll update and call other chunkservers to be updated
		 */
		if (System.currentTimeMillis() < CSMetadata.get(chunkhandle).getPrimaryLeaseTime() + LEASETIME) {
			File f = new File(chunkhandle); 
			long offset = 0;								
			try {
				RandomAccessFile raf = new RandomAccessFile(f, "rws");
				System.err.println("Atomic append seek length " + raf.length());
				raf.seek(raf.length());
				offset = raf.length();
				if (withSize) {
					ByteBuffer bb = ByteBuffer.allocate(4);
					bb.putInt(length);
					byte[] result = bb.array();
					raf.write(result);
				}
				raf.write(payload);
				raf.close();
			} catch (Exception e) {
				System.out.println("atomic append unsuccessful");
				e.printStackTrace();
				return false;
			}
			CSMetadata.get(chunkhandle).setWriteTime(System.currentTimeMillis());
			for (int i = 0; i < CSMetadata.get(chunkhandle).getSecondaries().size(); i++) {
				chunkservers.get(CSMetadata.get(chunkhandle).getSecondaries().get(i)).atomicAppendSecondary(chunkhandle, payload, length, withSize,offset );
			}
			return true;
		} else {
			System.out.println("Unable to append because not the primary");
			myClient.requestStatus("atomicAppend", chunkhandle, false, csIndex);
			return false;
		}
	}

	@Override
	public boolean atomicAppendSecondary(String chunkhandle, byte[] payload,
			int length, boolean withSize, long offset) throws RemoteException {
		File f = new File(chunkhandle); // might have to parse chunkhandle into

		try {
			RandomAccessFile raf = new RandomAccessFile(f, "rws");
			raf.seek(offset);
			if (withSize) {
				ByteBuffer bb = ByteBuffer.allocate(4);
				bb.putInt(length);
				byte[] result = bb.array();
				raf.write(result);
			}
			raf.write(payload);
			raf.close();
		} catch (Exception e) {
			System.out.println("atomic append unsuccessful");
			e.printStackTrace();
			return false;
		}

		return false;
	}

	public void fetchAndRewrite(String chunkhandle, int sourceID) throws RemoteException{

	}

	// called by master
	public void heartbeat() throws RemoteException{
		myMaster.heartbeat(csIndex);
	}

	public static void main(String args[]) {
		System.out.println("Enter ChunkServer ID.");
		System.out.print("> ");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String input = "";
		try {
			input = br.readLine();
		} catch (IOException e1) {
			System.out.println("Bad input. Try again.");
		}
		csIndex = Integer.parseInt(input);
		if(csIndex == 1) {
			System.out.println("Welcome Arjun, Cluster 36. Attempting to Connect...");
		}
		else if(csIndex == 2) {
			System.out.println("Welcome Brian, Cluster 05. Attempting to Connect...");
		}
		else if(csIndex == 3) {
			System.out.println("Welcome Julia, Cluster 29. Attempting to Connect...");
		}
		try {
			ChunkServer cs = new ChunkServer();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			System.out.println("Could not start chunkserver class instance");
		}

	}

	private class Metadata{
		long primaryLeaseTime;
		long writeTime;
		ArrayList<Integer> secondaries = new ArrayList<Integer>();
		String chunkhandle;

		public Metadata(String chunkhandle){
			writeTime = System.currentTimeMillis();
			primaryLeaseTime = -1;
			this.chunkhandle = chunkhandle;
		}

		public void setWriteTime(long time){
			this.writeTime = time;
		}

		public void setPrimaryLeaseTime(long time){
			primaryLeaseTime = time;
		}

		public void setSecondaries(ArrayList<Integer> secondaries){
			this.secondaries = secondaries;
		}

		public long getWriteTime(){
			return writeTime;
		}

		public long getPrimaryLeaseTime() {
			return primaryLeaseTime;
		}

		public ArrayList<Integer> getSecondaries() {
			return secondaries;
		}
	}
}