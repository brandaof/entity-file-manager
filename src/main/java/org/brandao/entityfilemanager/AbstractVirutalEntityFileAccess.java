package org.brandao.entityfilemanager;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;

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
		this.parent = e;
	}
	
	public void seek(long value) throws IOException{
		if(value > virtualLength)
			throw new IOException(file.getName() + ": entry not found: " + value);
		
		virtualOffset = value;
	}
	
	public void setLength(long value) throws IOException {
		virtualLength = value;
	}
	
	public long getOffset() throws IOException {
		return this.virtualOffset;
	}
	
	public long length() throws IOException {
		return virtualLength;
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
		super.batchWrite(entities, raw);
		
		for(int i=0;i<entities.length;i++){
			addVirutalOffset(virtualOffset + i, realOffset + i);
		}
		
		if(newVirtualOffset >= virtualLength){
			virtualLength = newVirtualOffset;
		}
		
	}
	
	protected Object read(boolean raw) throws IOException {
		
		Long realOffset = getOffset(virtualOffset);
		Object r        = null;
		
		if(realOffset == null){
			if(virtualOffset < parent.length()){
				parent.seek(virtualOffset);
				r = raw? parent.readRaw() : parent.read();
			}
		}
		else{
			offset = realOffset;
			r = super.read(raw);
		}
		
		virtualOffset++;
		
		return r;
	}
	
	protected Object[] batchRead(int len, boolean raw) throws IOException{
		
		int[] managedI    = new int[len];
		int[] notManagedI = new int[len];

		long[] managed      = new long[len];
		long[] notManaged   = new long[len];
		
		int notManagedIndex = 0;
		int managedIndex    = 0;
		
		Long realOffset;
		long localOffset;
		
		for(int i=0;i<len;i++){
			localOffset = virtualOffset + i;
			realOffset = getOffset(localOffset);
			if(realOffset == null){
				notManagedI[notManagedIndex]  = i;
				notManaged[notManagedIndex++] = localOffset;
			}
			else{
				managedI[managedIndex]  = i;
				managed[managedIndex++] = realOffset;
			}
		}
		
		Object[] managedE     = (Object[])Array.newInstance(
									raw? 
										this.dataHandler.getRawType() : 
										this.dataHandler.getType(), 
									managed.length
								);
		Object[] notmanagedE  = (Object[])Array.newInstance(
				raw? 
					this.dataHandler.getRawType() : 
					this.dataHandler.getType(), 
				notManaged.length
			);
		
		BulkOperations.read(managed, managedE, this, 0, managedIndex, raw);
		BulkOperations.read(notManaged, notmanagedE, parent, 0, notManagedIndex, raw);
		
		Object[] r = 
				(Object[])Array.newInstance(
						raw? 
							this.dataHandler.getRawType() : 
							this.dataHandler.getType(), 
						len
				);
		
		int i=0;
		
		for(Object o: managedE){
			r[managedI[i]] = o;
		}

		i=0;
		for(Object o: notmanagedE){
			r[notManagedI[i]] = o;
		}
		
		virtualOffset++;
		
		return r;
	}
	
	protected abstract void addVirutalOffset(long virtualOffset, long offset);

	protected abstract Long getOffset(long virutalOffset);
	
	public EntityFileAccess<T, R, H> getEntityFileAccess() {
		return parent;
	}
	
}
