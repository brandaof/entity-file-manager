package org.brandao.entityfilemanager;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import org.brandao.entityfilemanager.tx.EntityFileTransaction;
import org.brandao.entityfilemanager.tx.EntityFileTransactionManagerConfigurer;
import org.brandao.entityfilemanager.tx.EntityFileTransactionManagerImp;
import org.brandao.entityfilemanager.tx.TransactionException;

public class EntityFileManagerImpTest extends TestCase{

	private EntityFileManagerConfigurer efm;
	
	public void setUp(){
		File path   = new File("./data");
		File txPath = new File(path, "tx");
		
		EntityFileManagerConfigurer efm = new EntityFileManagerImp();
		
		LockProvider lp = new LockProviderImp();
		
		EntityFileTransactionManagerConfigurer tm = new EntityFileTransactionManagerImp();
		
		tm.setLockProvider(lp);
		tm.setTimeout(EntityFileTransactionManagerImp.DEFAULT_TIMEOUT);
		tm.setTransactionPath(txPath);
		tm.setEntityFileManagerConfigurer(efm);
		
		efm.setEntityFileTransactionManager(tm);
		efm.setLockProvider(lp);
		efm.setPath(path);
		efm.register("long", new LongEntityFileAccess(new File(path, "long")));
		efm.register("string", new StringEntityFileAccess(new File(path, "string")));
		
		efm.init();
		
		efm.truncate("long");
		efm.truncate("string");
		
		this.efm = efm;
	}
	
	public void tearDown(){
		this.efm.destroy();
	}
	
	public void testSimpleInsert() throws TransactionException, IOException{
		
		EntityFileTransaction tx = efm.beginTransaction();
		
		try{
			EntityFile<Long> ef = efm.getEntityFile("long", tx, Long.class);
			ef.insert(198563254512664L);
			assertEquals(198563254512664L, (long)ef.select(0));
		}
		finally{
			tx.commit();
		}
		
		tx = efm.beginTransaction();
		try{
			EntityFile<Long> ef = efm.getEntityFile("long", tx, Long.class);
			assertEquals(198563254512664L, (long)ef.select(0));
		}
		finally{
			tx.commit();
		}
		
	}

	public void testMultipleInsert() throws TransactionException, IOException{
		
		EntityFileTransaction tx = efm.beginTransaction();
		
		try{
			EntityFile<Long> ef = efm.getEntityFile("long", tx, Long.class);
			ef.insert(198563254512664L);
			ef.insert(152326598598562L);
			
			assertEquals(198563254512664L, (long)ef.select(0));
			assertEquals(152326598598562L, (long)ef.select(1));
			
		}
		finally{
			tx.commit();
		}
		
		tx = efm.beginTransaction();
		try{
			EntityFile<Long> ef = efm.getEntityFile("long", tx, Long.class);
			assertEquals(198563254512664L, (long)ef.select(0));
			assertEquals(152326598598562L, (long)ef.select(1));
		}
		finally{
			tx.commit();
		}
		
	}

	public void testInsertAndUpdate() throws TransactionException, IOException{
		
		EntityFileTransaction tx = efm.beginTransaction();
		
		try{
			EntityFile<Long> ef = efm.getEntityFile("long", tx, Long.class);
			ef.insert(198563254512664L);
			ef.insert(152326598598562L);
			
			assertEquals(198563254512664L, (long)ef.select(0));
			assertEquals(152326598598562L, (long)ef.select(1));
			
			ef.update(1, 0L);
			
			assertEquals(0L, (long)ef.select(1));
		}
		finally{
			tx.commit();
		}
		
		tx = efm.beginTransaction();
		try{
			EntityFile<Long> ef = efm.getEntityFile("long", tx, Long.class);
			assertEquals(198563254512664L, (long)ef.select(0));
			assertEquals(0L, (long)ef.select(1));
		}
		finally{
			tx.commit();
		}
		
	}

	public void testBulkInsert() throws TransactionException, IOException{
		
		EntityFileTransaction tx = efm.beginTransaction();
		
		try{
			EntityFile<Long> ef = efm.getEntityFile("long", tx, Long.class);
			ef.insert(new Long[]{new Long(1),new Long(2),new Long(3),new Long(4),new Long(5),new Long(6)});

			assertEquals(1L, (long)ef.select(0));
			assertEquals(2L, (long)ef.select(1));
			assertEquals(3L, (long)ef.select(2));
			assertEquals(4L, (long)ef.select(3));
			assertEquals(5L, (long)ef.select(4));
			assertEquals(6L, (long)ef.select(5));
		}
		finally{
			tx.commit();
		}
		
		tx = efm.beginTransaction();
		try{
			EntityFile<Long> ef = efm.getEntityFile("long", tx, Long.class);
			assertEquals(1L, (long)ef.select(0));
			assertEquals(2L, (long)ef.select(1));
			assertEquals(3L, (long)ef.select(2));
			assertEquals(4L, (long)ef.select(3));
			assertEquals(5L, (long)ef.select(4));
			assertEquals(6L, (long)ef.select(5));
		}
		finally{
			tx.commit();
		}
		
	}

	public void testBulkUpdate() throws TransactionException, IOException{
		
		EntityFileTransaction tx = efm.beginTransaction();
		
		try{
			EntityFile<Long> ef = efm.getEntityFile("long", tx, Long.class);
			Long[] values = new Long[]{new Long(1),new Long(2),new Long(3),new Long(4),new Long(5),new Long(6)};
			ef.insert(values);

			assertEquals(1L, (long)ef.select(0));
			assertEquals(2L, (long)ef.select(1));
			assertEquals(3L, (long)ef.select(2));
			assertEquals(4L, (long)ef.select(3));
			assertEquals(5L, (long)ef.select(4));
			assertEquals(6L, (long)ef.select(5));
		}
		finally{
			tx.commit();
		}

		tx = efm.beginTransaction();
		try{
			EntityFile<Long> ef = efm.getEntityFile("long", tx, Long.class);
			ef.update(
				new long[]{0,3,5}, 
				new Long[]{new Long(11),new Long(12),new Long(13)});
			assertEquals(11L, (long)ef.select(0));
			assertEquals(2L, (long)ef.select(1));
			assertEquals(3L, (long)ef.select(2));
			assertEquals(12L, (long)ef.select(3));
			assertEquals(5L, (long)ef.select(4));
			assertEquals(13L, (long)ef.select(5));
		}
		finally{
			tx.commit();
		}
		
		tx = efm.beginTransaction();
		try{
			EntityFile<Long> ef = efm.getEntityFile("long", tx, Long.class);
			assertEquals(11L, (long)ef.select(0));
			assertEquals(2L, (long)ef.select(1));
			assertEquals(3L, (long)ef.select(2));
			assertEquals(12L, (long)ef.select(3));
			assertEquals(5L, (long)ef.select(4));
			assertEquals(13L, (long)ef.select(5));
		}
		finally{
			tx.commit();
		}
		
	}
	
	public void testFail() throws TransactionException, IOException{
		
		EntityFileTransaction tx = efm.beginTransaction();
		
		try{
			EntityFile<Long> ef = efm.getEntityFile("long", tx, Long.class);
			Long[] values = new Long[]{new Long(1),new Long(2),new Long(3),new Long(4),new Long(5),new Long(6)};
			ef.insert(values);
			ef.update(1000, new Long(1));
			tx.commit();
		}
		catch(Throwable e){
			tx.rollback();
		}

		tx = efm.beginTransaction();
		try{
			EntityFile<Long> ef = efm.getEntityFile("long", tx, Long.class);
			assertEquals(0L, (long)ef.select(0));
			assertEquals(0L, (long)ef.select(1));
			assertEquals(0L, (long)ef.select(2));
			assertEquals(0L, (long)ef.select(3));
			assertEquals(0L, (long)ef.select(4));
			assertEquals(0L, (long)ef.select(5));
		}
		finally{
			tx.commit();
		}
		
	}
	
	public void testConcurrentUpdate() throws TransactionException, IOException, InterruptedException{
		EntityFileTransaction tx = efm.beginTransaction();
		
		EntityFile<Long> ef = efm.getEntityFile("long", tx, Long.class);
		ef.insert(152326598598562L);
		ef.insert(152326598598562L);
		
		new Thread(){
			
			public void run(){
				try{
					EntityFileTransaction tx = efm.beginTransaction();
					EntityFile<Long> ef = efm.getEntityFile("long", tx, Long.class);
					ef.update(0L, 12L);
					tx.commit();
				}
				catch(Throwable e){
					
				}
				
			}
			
		}.start();
		
		assertEquals(152326598598562L, (long)ef.select(0));

		tx.commit();
		
		Thread.sleep(1000);
		
		tx = efm.beginTransaction();

		ef = efm.getEntityFile("long", tx, Long.class);
		assertEquals(12L, (long)ef.select(0));
		assertEquals(152326598598562L, (long)ef.select(1));

		tx.commit();
		
	}
	
	public void testConcurrentInsert() throws Throwable{
		final int task                         = 100;
		final int ops                          = 3;
		final long sleep                       = 1000;
		final AtomicLong totalTime             = new AtomicLong(0);
		final CountDownLatch countDownLatch    = new CountDownLatch(task);
		final Random random                    = new Random();
		final ConcurrentMap<Long, Long> values = new ConcurrentHashMap<Long, Long>();
		final List<Throwable> ex               = new ArrayList<Throwable>();
		
		for(int i=0;i<task;i++){
			new Thread(){
				
				public void run(){
					try{
						EntityFileTransaction tx = efm.beginTransaction();
						EntityFile<Long> ef = efm.getEntityFile("long", tx, Long.class);
						
						sleep(2000 + random.nextInt(1000));

						long[] ids  = new long[ops];
						Long[] vals = new Long[ops];
						
						for(int i=0;i<ops;i++){
							vals[i] = new Long(random.nextLong());
						}
						
						long time = System.currentTimeMillis();
						for(int i=0;i<ops;i++){
							ids[i] = ef.insert(vals[i]);
							values.put(ids[i], vals[i]);
							sleep(sleep);
						}
						time = System.currentTimeMillis() - time;

						totalTime.addAndGet(time);
						
						for(int i=0;i<ops;i++){
							assertEquals(vals[i], ef.select(ids[i]));
						}
						
						tx.commit();
						
						tx = efm.beginTransaction();
						ef = efm.getEntityFile("long", tx, Long.class);
						
						for(int i=0;i<ops;i++){
							assertEquals(vals[i], ef.select(ids[i]));
						}
						tx.commit();
						
					}
					catch(Throwable e){
						ex.add(e);
					}
					finally{
						countDownLatch.countDown();
					}
					
				}
				
			}.start();
			
		}
		
		countDownLatch.await();
		
		if(!ex.isEmpty()){
			throw ex.get(0);
		}
		
		double time = totalTime.doubleValue() / (task*ops);
		assertTrue(time < (1.25*sleep));
		
		EntityFileTransaction tx = efm.beginTransaction();
		EntityFile<Long> ef      = efm.getEntityFile("long", tx, Long.class);
		
		for(Entry<Long,Long> v: values.entrySet()){
			long pv = ef.select(v.getKey());
			assertEquals("id: " + v.getKey() + " value: " + pv + " expected: " + v.getValue(), (long)v.getValue(), pv);
		}
		
	}
	
	public void testCommitMultipleFiles() throws TransactionException, IOException{
		
		EntityFileTransaction tx = efm.beginTransaction();
		
		try{
			EntityFile<Long> longEF     = efm.getEntityFile("long", tx, Long.class);
			EntityFile<String> stringEF = efm.getEntityFile("string", tx, String.class);
			
			longEF.insert(0L);
			longEF.insert(198563254512664L);
			longEF.insert(152326598598562L);

			stringEF.insert("test");
			stringEF.insert("test3");
			
			assertEquals(0L, (long)longEF.select(0));
			assertEquals(198563254512664L, (long)longEF.select(1));
			assertEquals(152326598598562L, (long)longEF.select(2));
			assertEquals("test", stringEF.select(0));
			assertEquals("test3", stringEF.select(1));
			
		}
		finally{
			tx.commit();
		}
		
		tx = efm.beginTransaction();
		
		try{
			EntityFile<Long> longEF     = efm.getEntityFile("long", tx, Long.class);
			EntityFile<String> stringEF = efm.getEntityFile("string", tx, String.class);
			
			assertEquals(0L, (long)longEF.select(0));
			assertEquals(198563254512664L, (long)longEF.select(1));
			assertEquals(152326598598562L, (long)longEF.select(2));
			assertEquals("test", stringEF.select(0));
			assertEquals("test3", stringEF.select(1));
			
		}
		finally{
			tx.commit();
		}
		
	}
	
}
