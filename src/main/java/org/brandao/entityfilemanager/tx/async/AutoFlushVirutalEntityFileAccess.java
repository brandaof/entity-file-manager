package org.brandao.entityfilemanager.tx.async;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.brandao.entityfilemanager.AbstractVirutalEntityFileAccess;
import org.brandao.entityfilemanager.EntityFileAccess;

public class AutoFlushVirutalEntityFileAccess<T, R, H>
	extends AbstractVirutalEntityFileAccess<T, R, H>{

	private ConcurrentMap<Long,Long> map;
	
	public AutoFlushVirutalEntityFileAccess(EntityFileAccess<T, R, H> e) throws IOException {
		super(e);
		this.map = new ConcurrentHashMap<Long, Long>();
	}
	
	@Override
	protected void addVirutalOffset(long virtualOffset, long offset) {
		map.put(virtualOffset, offset);
	}

	@Override
	protected Long getOffset(long virutalOffset) {
		return map.get(virutalOffset);
	}
	
	protected EntityFileAccess<T, R, H> getParent(){
		return parent;
	}

	protected EntityFileAccess<T, R, H> getVirtual(){
		return virtual;
	}
	
	public void resync() throws IOException{
		super.resync();
		map.clear();
	}

}
