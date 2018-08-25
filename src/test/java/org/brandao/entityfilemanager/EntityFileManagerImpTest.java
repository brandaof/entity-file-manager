package org.brandao.entityfilemanager;

import java.io.File;
import java.io.IOException;

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
		
		try{
			EntityFile<Long> ef = efm.getEntityFile("long", tx, Long.class);
			ef.insert(152326598598562L);
			
			new Thread(){
				
				public void run(){
					try{
						EntityFileTransaction tx = efm.beginTransaction();
						EntityFile<Long> ef = efm.getEntityFile("long", tx, Long.class);
						ef.update(0, 12L);
						tx.commit();
					}
					catch(Throwable e){
						
					}
					
				}
				
			}.start();
			
			assertEquals(198563254512664L, (long)ef.select(0));
		}
		finally{
			tx.commit();
		}
		
		Thread.sleep(1000);
		
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
	
	public void testCommitMoreFile() throws TransactionException, IOException{
		
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
