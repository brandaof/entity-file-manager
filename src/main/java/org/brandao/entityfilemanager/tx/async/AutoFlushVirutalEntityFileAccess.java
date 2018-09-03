package org.brandao.entityfilemanager.tx.async;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.brandao.entityfilemanager.AbstractVirutalEntityFileAccess;
import org.brandao.entityfilemanager.EntityFileAccess;

public class AutoFlushVirutalEntityFileAccess<T, R, H>
	extends AbstractVirutalEntityFileAccess<T, R, H>{

	private Map<Long,Long> map;
	
	public AutoFlushVirutalEntityFileAccess(EntityFileAccess<T, R, H> e) throws IOException {
		super(e);
		this.map = new HashMap<Long, Long>();
	}
	
	@Override
	protected void addVirutalOffset(long virtualOffset, long offset) {
		map.put(virtualOffset, offset);
	}

	@Override
	protected Long getOffset(long virutalOffset) {
		return map.get(virutalOffset);
	}
	
	public void resync() throws IOException{
		super.resync();
		map.clear();
	}

}
