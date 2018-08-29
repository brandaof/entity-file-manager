package org.brandao.entityfilemanager;

import java.io.IOException;
import java.io.RandomAccessFile;

public class BufferedWritterRandomAccessFile {
    
    private int offset;
    
    private byte[] buffer;
    
    private int capacity;
    
    private RandomAccessFile randomAccessFile;

    private boolean hasLineFeed;

    public BufferedWritterRandomAccessFile(RandomAccessFile randomAccessFile){
    	this(8192, randomAccessFile);
    }
    
    public BufferedWritterRandomAccessFile(int capacity, RandomAccessFile randomAccessFile){
        
        if(capacity < 256)
            throw new IllegalArgumentException("capacity");
        
        this.offset           = 0;
        this.buffer           = new byte[capacity];
        this.capacity         = capacity;
        this.randomAccessFile = randomAccessFile;
    }

    public void write(byte[] buffer) throws IOException{
        this.write(buffer, 0, buffer.length);
    }

    public void write(int i) throws IOException{
        this.write(new byte[]{(byte)(i & 0xff)}, 0, 1);
    }
    
    public void directWrite(byte[] buffer, int offset, int len) throws IOException{
        int limitOffset  = offset + len;
        
    	this.flush();
        
        while(offset < limitOffset){
            int maxRead  = limitOffset - offset;
            int maxWrite = this.capacity - this.offset;
            
            if(maxRead > maxWrite){
            	randomAccessFile.write(buffer, offset, maxWrite);
                offset      += maxWrite;
                this.offset += maxWrite;
                this.flush();
            }
            else{
            	randomAccessFile.write(buffer, offset, maxRead);
                offset       += maxRead;
                this.offset  += maxRead;
            }
        }
        
        //this.out.flush();
    }

    public void write(byte[] buffer, int offset, int len) throws IOException{
        int limitOffset  = offset + len;
        
        if(this.offset == this.capacity)
        	this.flush();
        
        while(offset < limitOffset){
            int maxRead  = limitOffset - offset;
            int maxWrite = this.capacity - this.offset;
            
            if(maxRead > maxWrite){
            	System.arraycopy(buffer, offset, this.buffer, this.offset, maxWrite);
                offset      += maxWrite;
                this.offset += maxWrite;
                this.flush();
            }
            else{
            	System.arraycopy(buffer, offset, this.buffer, this.offset, maxRead);
                offset       += maxRead;
                this.offset  += maxRead;
            }
        }
    } 

    public RandomAccessFile getDirectRandomAccessFile(){
    	return randomAccessFile;
    }
    
    public void flush() throws IOException{
    	if(this.offset > 0){
    		randomAccessFile.write(this.buffer, 0, this.offset);
	        //this.out.flush();
	        this.offset = 0;
    	}
    }

    public void clear(){
        this.offset = 0;
    }

    public boolean isHasLineFeed() {
        return hasLineFeed;
    }

}
