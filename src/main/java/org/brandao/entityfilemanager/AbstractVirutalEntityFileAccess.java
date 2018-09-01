package org.brandao.entityfilemanager;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;

import org.brandao.entityfilemanager.tx.EntityFileTransactionDataHandler;
import org.brandao.entityfilemanager.tx.EntityFileTransactionUtil;

public abstract class AbstractVirutalEntityFileAccess<T, R, H> 
	extends AbstractEntityFileAccess<T, R, H> {

	protected EntityFileAccess<T, R, H> parent;
	
	protected EntityFileTransactionDataHandler<T,R,H> transactionDataHandler;
	
	protected long virtualLength;
	
	protected long virtualOffset;
	
	protected ReadBulkTVirtual readBulkTVirtual;

	protected ReadBulkRVirtual readBulkRVirtual;
	
	public AbstractVirutalEntityFileAccess(EntityFileAccess<T, R, H> e, File file){
		super(
				e.getName(), 
				file, 
				e.getEntityFileDataHandler()
		);
		this.readBulkRVirtual = new ReadBulkRVirtual();
		this.readBulkTVirtual = new ReadBulkTVirtual();
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
	
	protected Object read(boolean raw) throws IOException {
		
		Long realOffset = getOffset(virtualOffset);
		Object r;
		
		if(realOffset == null){
			parent.seek(virtualOffset);
			r = raw? parent.readRaw() : parent.read();
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
		
		managed    = EntityFileTransactionUtil.adjustArray(managed, managedIndex);		
		notManaged = EntityFileTransactionUtil.adjustArray(notManaged, notManagedIndex);
		
		Object[] managedE;
		Object[] notmanagedE;
		
		managedE    = raw? readBulkRVirtual.read(managed) : readBulkTVirtual.read(managed);
		notmanagedE = raw? readBulkRVirtual.read(notManaged) : readBulkTVirtual.read(notManaged);
		
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

	@SuppressWarnings("unchecked")
	private class ReadBulkTVirtual extends ReadBulkArray<T>{

		protected T[] createArray(int len) {
			return (T[])Array.newInstance(dataHandler.getType());
		}

		protected T readItem(long id) throws IOException {
			offset = id;
			return (T)AbstractVirutalEntityFileAccess.super.read(false);
		}

		protected T[] readItens(long[] ids) throws IOException {
			offset = ids[0];
			return (T[])AbstractVirutalEntityFileAccess.super.batchRead(ids.length, false);
		}
		
	}

	@SuppressWarnings("unchecked")
	private class ReadBulkRVirtual extends ReadBulkArray<T>{

		protected T[] createArray(int len) {
			return (T[])Array.newInstance(dataHandler.getRawType());
		}

		protected T readItem(long id) throws IOException {
			offset = id;
			return (T)AbstractVirutalEntityFileAccess.super.read(false);
		}

		protected T[] readItens(long[] ids) throws IOException {
			offset = ids[0];
			return (T[])AbstractVirutalEntityFileAccess.super.batchRead(ids.length, false);
		}
		
	}
	
}
