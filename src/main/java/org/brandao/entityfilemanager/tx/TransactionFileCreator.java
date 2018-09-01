package org.brandao.entityfilemanager.tx;

import java.io.File;

public class TransactionFileCreator {

	private String prefix;
	
	private File path;
	
	private int index;
	
	private File currentFile;
	
	public TransactionFileCreator(String prefix, File path){
		this.prefix = prefix;
		this.path   = path;
		this.reloadIndex();
	}
	
	public void setIndex(int value){
		this.index = value;
	}
	
	public File getCurrentFile(){
		return this.currentFile;
	}

	public File getFileByIndex(int index){
		return new File(this.path, this.prefix + Long.toString(index, Character.MAX_RADIX));
	}
	
	public int getCurrentIndex(){
		return index;
	}
	
	public File getNextFile(){
		index++;
		this.currentFile = getFileByIndex(index);
		return this.currentFile;
	}
	
	private void reloadIndex(){
		
		index = -1;
		
		File[] files = path.listFiles();
		
		for(File f:files){
			
			String name = f.getName();
			
			if(name.startsWith(prefix)){
				
				String fileIndex = name.substring(prefix.length());
				int intFileIndex = Integer.parseInt(fileIndex, Character.MAX_RADIX);
				
				if(intFileIndex > index){
					index = intFileIndex;
				}
				
			}
			
		}
		
		if(index < 0){
			this.currentFile = null;
		}
		else{
			this.currentFile = getFileByIndex(index); 
		}
		
	}
	
}
