package Chunkserver;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ChunkserverMetadata implements Serializable {
	long primaryLeaseTime;
	long writeTime;
	List<Integer> secondaries = Collections.synchronizedList(new ArrayList<Integer>());
	String chunkhandle;

	public ChunkserverMetadata(String chunkhandle){
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

	public void setSecondaries(List<Integer> secondaries){
		this.secondaries = secondaries;
	}

	public long getWriteTime(){
		return writeTime;
	}

	public long getPrimaryLeaseTime() {
		return primaryLeaseTime;
	}

	public List<Integer> getSecondaries() {
		return secondaries;
	}

	static public void storeTree(Map<String, ChunkserverMetadata> cm) {
		String filename = "csmetadata";
		FileOutputStream fos = null;
		ObjectOutputStream out = null;
		try {
			fos = new FileOutputStream(filename);
			out = new ObjectOutputStream(fos);
			out.writeObject(cm);
			out.close();
			System.out.println("Chunkhandle Metadata Persisted");
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
	static public Map<String, ChunkserverMetadata> getMetadata() {
		String filename = "csmetadata";
		Map<String, ChunkserverMetadata> cm = null;
		// List pDetails = null;
		File f = new File(filename);
		if (!f.exists()) {
			System.out.println("No stored chunkserver metadata file exists.");
			return null;
		}
		FileInputStream fis = null;
		ObjectInputStream oin = null;
		try {
			if (f.length() > 0) {
				fis = new FileInputStream(filename);
				oin = new ObjectInputStream(fis);
				cm = (Map<String, ChunkserverMetadata>) oin.readObject();
				oin.close();
			}
		} catch (IOException ex) {
			System.out.println("Error in reading chunkserver metadata");
			ex.printStackTrace();
		} catch (ClassNotFoundException ex) {
			System.out.println("Error in reading chunkserver metadata");
			ex.printStackTrace();
		}
		return cm;
	}
}
