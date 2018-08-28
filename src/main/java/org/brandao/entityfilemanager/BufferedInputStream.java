package org.brandao.entityfilemanager;

import java.io.IOException;
import java.io.InputStream;

public class BufferedInputStream extends InputStream{
    
    private int offset;
    
    private int limit;
    
    private byte[] buffer;
    
    private int capacity;
    
    private InputStream stream;

    public BufferedInputStream(InputStream stream){
    	this(8192, stream);
    }
    
    public BufferedInputStream(int capacity, InputStream stream){
        this.offset   = 0;
        this.limit    = 0;
        this.buffer   = new byte[capacity];
        this.capacity = capacity;
        this.stream   = stream;
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
        
        int len = stream.read(this.buffer, this.limit, this.buffer.length - limit);
        
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
