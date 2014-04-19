package Chunkserver;

import java.util.ArrayList;

public class CSMetadata {
	ArrayList<CSMetadataItem> csMetaDataArray = new ArrayList<CSMetadataItem>();
	public CSMetadata(){
		
	}
	public void addMetaData(long startTime, int type, int stage, String chunkhandle){
		CSMetadataItem temp = new CSMetadataItem(startTime, type, stage, chunkhandle);
		csMetaDataArray.add(temp);
	}
}
