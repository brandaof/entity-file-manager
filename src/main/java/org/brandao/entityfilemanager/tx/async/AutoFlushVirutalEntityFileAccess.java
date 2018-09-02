package org.brandao.entityfilemanager.tx.async;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.brandao.entityfilemanager.AbstractVirutalEntityFileAccess;
import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.tx.Await;

public class AutoFlushVirutalEntityFileAccess<T, R, H>
	extends AbstractVirutalEntityFileAccess<T, R, H>{

	private Map<Long,Long> map;
	
	private Await await;
	
	private boolean closed;
	
	public AutoFlushVirutalEntityFileAccess(EntityFileAccess<T, R, H> e, File file,
			Await await) {
		super(e, file);
		this.map            = new HashMap<Long, Long>();
		this.await          = await;
		this.closed         = false;
		Thread task         = 
			new Thread(
					null, 
					new AutoFlushVirutalEntityFileAccessTask(), 
					"Entity File " + e.getName() + " flush task");
		task.start();
	}
	
	@Override
	protected void addVirutalOffset(long virtualOffset, long offset) {
		map.put(virtualOffset, offset);
	}

	@Override
	protected Long getOffset(long virutalOffset) {
		return map.get(virutalOffset);
	}

	public void close() throws IOException{
		try{
			super.close();
		}
		finally{
			closed = true;
		}
	}

	protected synchronized void write(Object entity, boolean raw) throws IOException {
		super.write(entity, raw);
	}
	
	protected synchronized void batchWrite(Object[] entities, boolean raw) throws IOException{
		super.batchWrite(entities, raw);
	}
	
	protected synchronized Object read(boolean raw) throws IOException {
		return super.read(raw);
	}

	protected synchronized Object[] batchRead(int len, boolean raw) throws IOException{
		return super.batchRead(len, raw);
	}
	
	public synchronized void reset() throws IOException{
		super.reset();
		map.clear();
	}
	
	public Await getAwait() {
		return await;
	}

	private class AutoFlushVirutalEntityFileAccessTask 
		implements Runnable{

		public void run() {
			while(!closed){
				try{
					await.waitAll();
					if(!map.isEmpty()){
						reset();
					}
					else{
						Thread.sleep(1000);
					}
				}
				catch(Throwable e){
					e.printStackTrace();
				}
			}
		}
		
	}
}
