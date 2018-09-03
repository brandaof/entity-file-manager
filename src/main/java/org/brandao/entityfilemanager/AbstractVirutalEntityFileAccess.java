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
	
	protected void write(Object[] b, int off, int len, boolean raw) throws IOException{
		
		int last              = off + len;
		long newVirtualOffset = virtualOffset + len;
		long realOffset       = offset;
		
		offset = length;
		super.write(b, off, len, raw);
		
		for(int i=off;i<last;i++){
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
	
	protected int read(Object[] b, int off, int len, boolean raw) throws IOException{
		
		long maxRead = virtualLength - virtualOffset;
		len          = maxRead > len? len : (int)maxRead;
		
		IdMap[] ids = getMappedIds(virtualOffset, len);
		
		Object[] notmanagedE  = (Object[])Array.newInstance(
				raw? 
					this.dataHandler.getRawType() : 
					this.dataHandler.getType(), 
				ids[0].len
			);

		Object[] managedE     = (Object[])Array.newInstance(
				raw? 
					this.dataHandler.getRawType() : 
					this.dataHandler.getType(), 
				ids[1].len
			);
		
		BulkOperations.read(ids[0].ids, notmanagedE, parent, 0, ids[0].len, raw);
		BulkOperations.read(ids[1].ids, managedE, this, 0, ids[1].len, raw);
		
		int i=0;
		
		for(Object o: notmanagedE){
			b[off + ids[0].map[i++]] = o;
		}

		i=0;
		
		for(Object o: managedE){
			b[off + ids[1].map[i++]] = o;
		}
		
		virtualOffset++;
		
		return len;
	}
	
	protected abstract void addVirutalOffset(long virtualOffset, long offset);

	protected abstract Long getOffset(long virutalOffset);
	
	public EntityFileAccess<T, R, H> getEntityFileAccess() {
		return parent;
	}
	
	private IdMap[] getMappedIds(long id, int len){
		
		IdMap managedID = new IdMap();
		managedID.ids   = new long[len];
		managedID.map   = new int[len];
		managedID.len   = 0;
		
		IdMap notManagedID = new IdMap();
		notManagedID.ids   = new long[len];
		notManagedID.map   = new int[len];
		notManagedID.len   = 0;
		
		Long realOffset;
		long localOffset;
		
		for(int i=0;i<len;i++){
			
			localOffset = id + i;
			realOffset  = getOffset(localOffset);
			
			if(realOffset == null){
				managedID.map[managedID.len  ] = i; 
				managedID.ids[managedID.len++] = localOffset;
			}
			else{
				notManagedID.map[notManagedID.len  ] = i; 
				notManagedID.ids[notManagedID.len++] = realOffset;
			}
			
		}
		
		return new IdMap[]{notManagedID, managedID};
	}
	
	private static class IdMap{
		
		public long[] ids;
		
		public int[] map;

		public int len;
		
	}
	
}
