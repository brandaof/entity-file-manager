package org.brandao.entityfilemanager;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.concurrent.locks.Lock;

public abstract class AbstractVirutalEntityFileAccess<T, R, H> 
	implements EntityFileAccess<T, R, H> {
	
	protected EntityFileAccess<T, R, H> parent;
	
	protected EntityFileAccess<T, R, H> virtual;
	
	protected long virtualLength;
	
	protected long virtualOffset;
	
	public AbstractVirutalEntityFileAccess(EntityFileAccess<T, R, H> e) throws IOException{
		this.virtualLength = e.length();
		this.virtualOffset = 0;
		this.parent  = e;
		this.virtual = 
				new SimpleEntityFileAccess<T, R, H>(
					e.getName(), 
					new File(e.getAbsolutePath() + "_virt"), 
					e.getEntityFileDataHandler()
				);
	}
	
	public void resync() throws IOException{
		virtual.reset();
		virtualLength = parent.length();
		virtualOffset = 0;
	}
	
	public void reset() throws IOException {
		virtual.reset();
	}
	
	public Class<T> getType() {
		return parent.getType();
	}
	
	public Class<R> getRawType() {
		return parent.getRawType();
	}
	
	public H getMetadata() {
		return parent.getMetadata();
	}
	
	public EntityFileDataHandler<T, R, H> getEntityFileDataHandler() {
		return parent.getEntityFileDataHandler();
	}
	
	public void setBatchLength(int value) {
		virtual.setBatchLength(value);
	}
	
	public int getBatchLength() {
		return virtual.getBatchLength();
	}
	
	public Lock getLock() {
		return virtual.getLock();
	}
	
	public File getAbsoluteFile() {
		return virtual.getAbsoluteFile();
	}
	
	public String getAbsolutePath() {
		return virtual.getAbsolutePath();
	}
	
	public String getName() {
		return parent.getName();
	}
	
	public void createNewFile() throws IOException {
		virtual.createNewFile();
	}
	
	public void open() throws IOException {
		virtual.open();
	}
	
	public void seek(long value) throws IOException {
		if(value > virtualLength)
			throw new IOException(virtual.getName() + ": entry not found: " + value);
		
		virtualOffset = value;
	}
	
	public long getOffset() throws IOException {
		return this.virtualOffset;
	}
	
	public void batchWrite(T[] entities) throws IOException {
		write(entities, 0, entities.length, false);
	}
	
	public void write(T value) throws IOException {
		Object[] o = new Object[]{value};
		write(o, 0, 1, false);
	}
	
	public void writeRaw(R value) throws IOException {
		Object[] o = new Object[]{value};
		write(o, 0, 1, true);
	}
	
	public void write(T[] b, int off, int len) throws IOException {
		write(b, off, len, false);
	}
	
	public void writeRaw(R[] b, int off, int len) throws IOException {
		write(b, off, len, true);
	}
	
	public void batchWriteRaw(R[] entities) throws IOException {
		write(entities, 0, entities.length, true);
	}
	
	@SuppressWarnings("unchecked")
	protected void write(Object[] b, int off, int len, boolean raw) throws IOException{
		
		int last              = off + len;
		long newVirtualOffset = virtualOffset + len;
		long realOffset       = virtual.length();
		
		if(raw){
			virtual.writeRaw((R[]) b, off, len);
		}
		else{
			virtual.write((T[]) b, off, len);
		}
		
		for(int i=off;i<last;i++){
			addVirutalOffset(virtualOffset + i, realOffset + i);
		}
		
		if(newVirtualOffset >= virtualLength){
			virtualLength = newVirtualOffset;
		}
		
	}
	
	@SuppressWarnings("unchecked")
	public T[] batchRead(int len) throws IOException {
		return (T[]) batchRead(len, false);
	}
	
	@SuppressWarnings("unchecked")
	public T read() throws IOException {
		Object[] b = new Object[1];
		read(b, 0, 1, false);
		return (T) b[0];
	}
	
	@SuppressWarnings("unchecked")
	public R readRaw() throws IOException {
		Object[] b = new Object[1];
		read(b, 0, 1, true);
		return (R) b[0];
	}
	
	@SuppressWarnings("unchecked")
	public R[] batchReadRaw(int len) throws IOException {
		return (R[]) batchRead(len, true);
	}
	
	public int read(T[] b, int off, int len) throws IOException {
		return read(b, off, len, false);
	}
	
	public int readRaw(R[] b, int off, int len) throws IOException {
		return read(b, off, len, true);
	}
	
	protected Object[] batchRead(int len, boolean raw) throws IOException{
		
		long maxRead = virtualLength - virtualOffset;
		len          = maxRead > len? len : (int)maxRead;
	
		Object[] result = 
				(Object[])Array.newInstance(
						raw? 
							virtual.getRawType() : 
							virtual.getType(), 
						len
				);
		
		read(result, 0, len, raw);
		return result;
	}
	
	protected int read(Object[] b, int off, int len, boolean raw) throws IOException{
		
		long maxRead = virtualLength - virtualOffset;
		len          = maxRead > len? len : (int)maxRead;
		
		IdMap[] ids = getMappedIds(virtualOffset, len);
		
		Object[] notmanagedE  = (Object[])Array.newInstance(
				raw? 
					virtual.getRawType() : 
					virtual.getType(), 
				ids[0].len
			);
	
		Object[] managedE     = (Object[])Array.newInstance(
				raw? 
					virtual.getRawType() : 
					virtual.getType(), 
				ids[1].len
			);
		
		BulkOperations.read(ids[0].ids, notmanagedE, parent, 0, ids[0].len, raw, true);
		BulkOperations.read(ids[1].ids, managedE, virtual, 0, ids[1].len, raw, false);
		
		int i=0;
		
		for(Object o: notmanagedE){
			b[off + ids[0].map[i++]] = o;
		}
	
		i=0;
		
		for(Object o: managedE){
			b[off + ids[1].map[i++]] = o;
		}
		
		virtualOffset += len;
		
		return len;
	}
	
	public long length() throws IOException {
		return virtualLength;
	}
	
	public void setLength(long value) throws IOException {
		virtualLength = value;
	}
	
	public boolean exists() {
		return virtual.exists();
	}
	
	public void flush() throws IOException {
		virtual.flush();
	}
	
	public void close() throws IOException {
		virtual.close();
	}
	
	public void delete() throws IOException {
		virtual.delete();
	}
	
	protected abstract void addVirutalOffset(long virtualOffset, long offset);
	
	protected abstract Long getOffset(long virutalOffset);
	
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
				notManagedID.map[notManagedID.len  ] = i; 
				notManagedID.ids[notManagedID.len++] = localOffset;
			}
			else{
				managedID.map[managedID.len  ] = i; 
				managedID.ids[managedID.len++] = realOffset;
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
