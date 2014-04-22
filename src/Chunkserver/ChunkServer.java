package Chunkserver;

import java.io.File;
import java.io.IOException;
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
import Interfaces.MasterInterface;
import Utilities.Tree;

public class ChunkServer extends UnicastRemoteObject implements
		ChunkserverInterface {
	// public CSMetadata csmd = new CSMetadata();
	Map<String, Long> CSMetaData = new HashMap<String, Long>();
	MasterInterface myMaster;

	public ChunkServer() throws RemoteException {
		//setupMasterChunkserverHost();
		//setupMasterChunkserverClient();

	}

	//Master calls Chunkserver methods -> MASTERCHUNK1
	public void setupMasterChunkserverHost() {
		try {
			System.setSecurityManager(new RMISecurityManager());
			Registry registry = LocateRegistry.createRegistry(1099);
			Naming.rebind("rmi://dblab-18.vlab.usc.edu/MASTERCHUNK1", this);
		} catch (MalformedURLException re) {
			System.out.println("Bad connection");
			re.printStackTrace();
		} catch (RemoteException e) {
			System.out.println("Bad connection");
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	//Chunkserver calls Master Methods -> CHUNKMASTER1
	public void setupMasterChunkserverClient() {
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
					.lookup("rmi://dblab-29.vlab.usc.edu/CHUNKMASTER1");
			myMaster.setupMasterChunkserverClient();

			/*
			 * ChunkServer FUNCTION HOST implementation
			 */

		} catch (Exception re) {
			re.printStackTrace();
		}
	}

	@Override
	public Map<String, Long> refreshMetadata() throws RemoteException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void primaryLease(String chunkhandle) throws RemoteException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean createFile(String chunkhandle) throws RemoteException {
		File f = new File(chunkhandle);
		try {
			if (!f.createNewFile()) {
				System.err.println("File creation unsuccessful " + chunkhandle);
				return false;
			}
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
		try {
			RandomAccessFile raf = new RandomAccessFile(f, "rws");
			raf.seek(offset);
			raf.write(payload);
			raf.close();
		} catch (IOException e) {
			System.out.println("Append was unsuccessful");
			e.printStackTrace();
			return false;
		}
		// TODO : add to metadata
		CSMetaData.put(chunkhandle, System.currentTimeMillis());
		return true;
	}

	@Override
	public boolean atomicAppend(String chunkhandle, byte[] payload, int length,
			boolean withSize) throws RemoteException {
		File f = new File(chunkhandle); // might have to parse chunkhandle into
										// path
		try {
			RandomAccessFile raf = new RandomAccessFile(f, "rws");
			raf.seek(raf.length());
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
		// TODO : add to metadata
		CSMetaData.put(chunkhandle, System.currentTimeMillis());
		return true;
	}

	@Override
	public boolean atomicAppendSecondary(String chunkhandle, byte[] payload,
			int length, boolean withSize, int offset) throws RemoteException {
		// TODO Auto-generated method stub
		return false;
	}

	public static void main(String args[]) {
		try {
			ChunkServer cs = new ChunkServer();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}