package org.brandao.entityfilemanager;

import java.io.File;
import java.io.IOException;

import org.brandao.entityfilemanager.tx.EntityFileTransactionDataHandler;

public abstract class AbstractVirutalEntityFileAccess<T, R, H> 
	extends AbstractEntityFileAccess<T, R, H> {

	protected EntityFileAccess<T, R, H> parent;
	
	protected EntityFileTransactionDataHandler<T,R,H> transactionDataHandler;
	
	protected long virtualLength;
	
	protected long virtualOffset;
	
	public AbstractVirutalEntityFileAccess(EntityFileAccess<T, R, H> e, File file){
		super(
				e.getName(), 
				file, 
				e.getEntityFileDataHandler()
		);
	}
	
	public void seek(long value) throws IOException{
		if(value > virtualLength)
			throw new IOException(file.getName() + ": entry not found: " + value);
		
		virtualOffset = value;
	}
	
	public void setLength(long value) throws IOException {
		virtualLength = value;
	}
	
	protected void write(Object entity, boolean raw) throws IOException {
 
		long newVirtualOffset = virtualOffset + 1;
		long realOffset       = offset;
		
		offset = length;
		super.write(entity, raw);
		
		addVirutalOffset(virtualOffset, realOffset);
		
		if(newVirtualOffset >= virtualLength){
			virtualLength++;
		}
		
	}
	
	protected void batchWrite(Object[] entities, boolean raw) throws IOException{
		
		long newVirtualOffset = virtualOffset + entities.length;
		long realOffset       = offset;
		
		offset = length;
		super.write(entities, raw);
		
		for(int i=0;i<entities.length;i++){
			addVirutalOffset(newVirtualOffset + i, realOffset + i);
		}
		
		if(newVirtualOffset >= virtualLength){
			virtualLength = newVirtualOffset;
		}
		
	}
	
	protected abstract void addVirutalOffset(long virtualOffset, long offset);

	protected abstract long getVirtualOffset(long offset);
	
	public EntityFileAccess<T, R, H> getEntityFileAccess() {
		return parent;
	}
	
}
