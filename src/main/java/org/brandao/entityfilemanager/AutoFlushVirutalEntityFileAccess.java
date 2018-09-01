package org.brandao.entityfilemanager;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class AutoFlushVirutalEntityFileAccess<T, R, H>
	extends AbstractVirutalEntityFileAccess<T, R, H>{

	private Map<Long,Long> map;
	
	private CountDownLatch countDownLatch;
	
	private boolean closed;
	
	public AutoFlushVirutalEntityFileAccess(EntityFileAccess<T, R, H> e, File file,
			CountDownLatch countDownLatch) {
		super(e, file);
		this.map            = new HashMap<Long, Long>();
		this.countDownLatch = countDownLatch;
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

	protected void write(Object entity, boolean raw) throws IOException {
		super.write(entity, raw);
	}
	
	protected void batchWrite(Object[] entities, boolean raw) throws IOException{
		super.write(entities, raw);
	}
	
	protected synchronized Object read(boolean raw) throws IOException {
		return super.read(raw);
	}

	protected Object[] batchRead(int len, boolean raw) throws IOException{
		return super.batchRead(len, raw);
	}
	
	public synchronized void reset() throws IOException{
		super.reset();
		map.clear();
	}
	
	private class AutoFlushVirutalEntityFileAccessTask 
		implements Runnable{

		public void run() {
			while(!closed){
				try{
					countDownLatch.await();
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
