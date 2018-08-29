package org.brandao.entityfilemanager;

import java.io.IOException;
import java.io.RandomAccessFile;

public class BufferedReaderRandomAccessFile {
    
    private int offset;
    
    private int limit;
    
    private byte[] buffer;
    
    private int capacity;
    
    private RandomAccessFile randomAccessFile;

    public BufferedReaderRandomAccessFile(RandomAccessFile randomAccessFile){
    	this(8192, randomAccessFile);
    }
    
    public BufferedReaderRandomAccessFile(int capacity, RandomAccessFile randomAccessFile){
        this.offset           = 0;
        this.limit            = 0;
        this.buffer           = new byte[capacity];
        this.capacity         = capacity;
        this.randomAccessFile = randomAccessFile;
    }

    public int available() throws IOException {
    	return this.limit - this.offset;
    }
    
    public int read() throws IOException{
    	
        if(this.offset == this.limit && this.checkBuffer() < 0){
        	return -1;
        }
    	
        return this.buffer[this.offset++];
    }
    
    public int read(byte[] b, int off, int len) throws IOException{
    	
    	int read  = 0;
    	
    	while(len > 0){
    		
            int maxRead = this.limit - this.offset;
            
            if(len > maxRead){
            	
                if(this.offset == this.limit && this.checkBuffer() < 0){
                	return read;
                }
                
            	System.arraycopy(this.buffer, this.offset, b, off, maxRead);
            	this.offset += maxRead;
            	off         += maxRead;
            	read        += maxRead;
            	len         -= maxRead;
            }
            else{
            	System.arraycopy(this.buffer, this.offset, b, off, len);
            	this.offset += len;
            	read        += len;
            	return read; 
            }
            
    	}
    	
    	return read;
    }
    
    private int checkBuffer() throws IOException{
        if(this.limit == this.capacity){
            this.offset = 0;
            this.limit  = 0;
        }
        
        int len = randomAccessFile.read(this.buffer, this.limit, this.buffer.length - limit);
        
        if(len == -1){
        	return -1;
        }
        
        this.limit += len;
        return len;
    }
    
    public void clear(){
        this.offset = 0;
        this.limit  = 0;
    }

}
