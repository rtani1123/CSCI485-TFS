package Chunkserver;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.rmi.Naming;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import Interfaces.ChunkserverInterface;
import Interfaces.ClientInterface;
import Interfaces.MasterInterface;

public class ChunkServer extends UnicastRemoteObject implements ChunkserverInterface {
	Map<String, ChunkserverMetadata> CSMetadata = Collections.synchronizedMap(new HashMap<String, ChunkserverMetadata>());
	Map<Integer, ChunkserverInterface> chunkservers  = Collections.synchronizedMap(new HashMap<Integer, ChunkserverInterface>());
	MasterInterface myMaster;
//	ClientInterface myClient;
	Map<Integer, ClientInterface> clients;
	static Integer csIndex;
	public static final long LEASETIME = 60000;

	public ChunkServer() throws RemoteException {
		clients = Collections.synchronizedMap(new HashMap<Integer, ClientInterface>());
		try {
			if(ChunkserverMetadata.getMetadata()!=null) {
				CSMetadata = ChunkserverMetadata.getMetadata();
			}
		} catch(Exception e) {
			System.out.println("No Chunkserver Metadata found for " + csIndex);
		}
		
		chunkservers = Collections.synchronizedMap(new HashMap<Integer, ChunkserverInterface>());
		setupChunkserverHost();

		connectToMaster();
		myMaster.connectToChunkserver(csIndex);
		
	}

	/**
	 * URL Protocol to establish RMI Server on ChunkserverSide.
	 */
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
			System.out.println("Chunkserver unable to connect");
		} catch (Exception e) {		}
	}

	/**
	 * Connecting ChunkServer(csIndex) to Master, RMI.
	 */
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
			myMaster = (MasterInterface) Naming.lookup("rmi://dblab-18.vlab.usc.edu/MASTER");
			System.out.println("Connection to Master Success");

		} catch (Exception re) {
			System.out.println("Master failure to host.");
		}
	}

	/**
	 * Connecting ChunkServer(csIndex) to Client(index)
	 * @param index
	 */
	public void connectToClient(Integer id) {
		try {
			System.setSecurityManager(new RMISecurityManager());

			ClientInterface myClient = (ClientInterface)Naming.lookup("rmi://dblab-43.vlab.usc.edu:" + id + "/CLIENT" + id);
			clients.put(id, myClient);
			System.out.println("Connection to Client " + id + " Success");

		} catch(Exception re) {
			System.out.println("Client failure to host");
		}
	}

	/**
	 * Connects ChunkServer(csIndex) to Client(index)
	 * @param index
	 */
	public void connectToChunkserver(Integer id) {
		try {
			System.setSecurityManager(new RMISecurityManager());
			ChunkserverInterface tempCS = null;
			System.out.println("Connected to " + id);
			if(id == 1)
				tempCS = (ChunkserverInterface)Naming.lookup("rmi://dblab-36.vlab.usc.edu:123/CHUNK" + id.toString());
			else if(id == 2)
				tempCS = (ChunkserverInterface)Naming.lookup("rmi://dblab-05.vlab.usc.edu:124/CHUNK" + id.toString());
			else if(id == 3)
				tempCS = (ChunkserverInterface)Naming.lookup("rmi://dblab-29.vlab.usc.edu:125/CHUNK" + id.toString());
			chunkservers.put(id, tempCS);
			
			/*
			 * ChunkServer FUNCTION HOST implementation
			 */

		} catch(Exception re) {
			System.out.println("Failed connection to Chunkserver " + id);
		}
	}
	
//	/
//	@Override
//	public Map<String, Long> refreshMetadata() throws RemoteException {
//		// TODO Auto-generated method stub
//		return null;
//	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see Interfaces.ChunkserverInterface#primaryLease(java.lang.String,
	 * java.util.ArrayList)
	 */
	/**
	 * Sets CSMetadata primary lease times as well as appropriate secondary ChunkServers
	 * @param chunkhandle
	 * @param CServers
	 * 
	 */
	@Override
	public void primaryLease(String chunkhandle, List<Integer> CServers)
			throws RemoteException {
		CSMetadata.get(chunkhandle).setPrimaryLeaseTime(System.currentTimeMillis());
		CSMetadata.get(chunkhandle).setSecondaries(CServers);
	}
	
	/**
	 * Creates a file locally to ChunkServer.
	 * Chunkserver then stores that data in a local metadata file.
	 * @param chunkhandle
	 */
	@Override
	public boolean createFile(String chunkhandle) throws RemoteException {
		File f = new File(chunkhandle);
		try {
			if (!f.createNewFile()) {
				System.err.println("File creation unsuccessful " + chunkhandle);
				return false;
			}
			ChunkserverMetadata md = new ChunkserverMetadata(chunkhandle);
			CSMetadata.put(chunkhandle, md);
			ChunkserverMetadata.storeTree(CSMetadata);
		} catch (Exception e) {
			System.out.println("File creation unsuccessful");
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * Creates a directory locally to ChunkServer.
	 * Chunkserver then stores that data in a local metadata file.
	 * @param chunkhandle
	 */
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

	/**
	 * Deletes a file locally from ChunkServer.
	 * Chunkserver then stores that information in a local metadata file.
	 * @param chunkhandle
	 */
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

				CSMetadata.remove(chunkhandle);
				ChunkserverMetadata.storeTree(CSMetadata);
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
		CSMetadata.remove(chunkhandle);
		ChunkserverMetadata.storeTree(CSMetadata);
		return true;
	}

	/**
	 * Deletes a directory locally from ChunkServer.
	 * Chunkserver then stores that information in a local metadata file.
	 * @param chunkhandle
	 */
	@Override
	public boolean deleteDirectory(String chunkhandle) throws RemoteException {
		if (deleteFile(chunkhandle)) {

		} else {
			System.out.println("Delete unsuccessful. Item not found.");
			return false;
		}
		return true;
	}
	
	/**
	 * Read file completely
	 * @param chunkhandle
	 */
	@Override
	public byte [] readCompletely(String chunkhandle) throws RemoteException{
		File f = new File(chunkhandle);	
		
		byte[] b = new byte[(int)f.length()];
		try {
			RandomAccessFile raf = new RandomAccessFile(f,"r");
			raf.readFully(b);
			raf.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("File read exception");
			return new byte[0];
		}
		return b;
	}
	
	/**
	 * Read file at an offset
	 * @param chunkhandle
	 * @param offset
	 * @param length
	 */
	@Override
	public byte[] read(String chunkhandle, int offset, int length) throws RemoteException {
		File f = new File(chunkhandle); 
		if(offset > f.length()){
			System.out.println("Unable to read because offset exceeds file length");
			return new byte[0];
		}
		else if ((offset + length) > f.length()){
			length = (int)f.length() - offset;
			System.out.println("Truncating out of bounds read.");
		}
		byte[] b = new byte[length];
		try {
			RandomAccessFile raf = new RandomAccessFile(f, "r");
			raf.seek(offset);
			raf.readFully(b);
			raf.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error in creating RandomAccessFile in read. Returning byte array of 0 size.");
			return new byte[0];
		}
		return b;
	}

	/**
	 * Appending to file at location chunkhandle.
	 * Data: payload
	 * Length and offset
	 * @param chunkhandle
	 * @param payload
	 * @param length
	 * @param offset
	 * @param withSize
	 */
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
		CSMetadata.get(chunkhandle).setWriteTime(System.currentTimeMillis());
		ChunkserverMetadata.storeTree(CSMetadata);
		return true;
	}
	
	/**
	 * Atomic append to file at location chunkhandle.
	 * Data: payload
	 * Length, but no offset specified. (determined by TFS)
	 * @param chunkhandle
	 * @param payload
	 * @param length
	 * @param withSize
	 */
	@Override
	public boolean atomicAppend(String chunkhandle, byte[] payload, int length,
			boolean withSize) throws RemoteException {
		/*
		 * If it's primary, it'll update and call other chunkservers to be updated
		 */
		if (System.currentTimeMillis() < CSMetadata.get(chunkhandle).getPrimaryLeaseTime() + LEASETIME) {
			System.out.println("This is a primary");
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
			ChunkserverMetadata.storeTree(CSMetadata);
			for (int i = 0; i < CSMetadata.get(chunkhandle).getSecondaries().size(); i++) {
				try{
					System.out.println("The chunkhandle is: " + CSMetadata.get(chunkhandle));
					System.out.println("Number of Secondaries: " + CSMetadata.get(chunkhandle).getSecondaries());
					System.out.println("My chunkserver keys: " + chunkservers.keySet());
					chunkservers.get(CSMetadata.get(chunkhandle).getSecondaries().get(i)).atomicAppendSecondary(chunkhandle, payload, length, withSize,offset );
				}
				catch (RemoteException re){
					System.out.println("Unable to push to secondary " + CSMetadata.get(chunkhandle).getSecondaries().get(i) + " because unavailable.");
				}
			}
			return true;
		} else {
			System.out.println("Unable to append because not the primary");
			return false;
		}
	}

	/**
	 * Called from the primary chunkserver.
	 * Pushing data to secondary.
	 * @param chunkhandle
	 * @param payload
	 * @param length
	 * @param withSize
	 */
	@Override
	public boolean atomicAppendSecondary(String chunkhandle, byte[] payload,
			int length, boolean withSize, long offset) throws RemoteException {
		System.out.println("This is a secondary");
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

	/**
	 * Chunkserver recovery append.
	 * @param chunkhandle
	 * @param sourceID
	 */
	public void fetchAndRewrite(String chunkhandle, int sourceID) throws RemoteException{
		try{
			byte [] payload = chunkservers.get(sourceID).readCompletely(chunkhandle);
			append(chunkhandle, payload, payload.length, 0, false);
		}
		catch (RemoteException re){
			System.out.println("Could not connect to alternative data source for append.");
		}
		catch (Exception e){
			System.out.println("Fetch and rewrite failure");
		}
	}

	/**
	 * Heartbeat message.
	 */
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

	

	
}