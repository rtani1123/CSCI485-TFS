package Chunkserver;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import Interfaces.ChunkserverInterface;
import Utilities.Tree;

public class ChunkServer implements ChunkserverInterface {
//	public CSMetadata csmd = new CSMetadata();
	Map<String, Long> CSMetaData = new HashMap<String, Long>();
	public ChunkServer() {
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
			}
		} catch (Exception e) {
			e.printStackTrace();
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
			e.printStackTrace();
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
			if (dFile.isFile() || (files.length == 0))
				if (!dFile.delete())
					return false;
				else if (dFile.isDirectory()) {
					for (int i = 0; i < files.length; i++) {
						deleteFile(parsedPath + "/" + files[i]);
					}
					if (!dFile.delete())
						return false;
				}

		} catch (Exception e) {
			e.printStackTrace();
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
			e.printStackTrace();
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
			ByteBuffer bb = ByteBuffer.allocate(4);
			bb.putInt(length);
			byte[] result = bb.array();
			raf.write(result);
			raf.write(payload);
			raf.close();
		} catch (Exception e) {
			e.printStackTrace();
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

}