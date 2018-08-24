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
		tm.setTimeout(EntityFileTransactionManagerImp.DEFAULY_TIME_OUT);
		tm.setTransactionPath(txPath);
		tm.setEntityFileManagerConfigurer(efm);
		
		efm.setEntityFileTransactionManager(tm);
		efm.setLockProvider(lp);
		efm.setPath(path);
		efm.register("long", new LongEntityFileAccess(new File(path, "long")));
		efm.init();
		
		this.efm = efm;
	}
	
	public void tearDown(){
		this.efm.destroy();
	}
	
	public void testCommitOneFile() throws TransactionException, IOException{
		
		EntityFileTransaction tx = efm.beginTransaction();
		
		try{
			EntityFile<Long> ef = efm.getEntityFile("long", tx, Long.class);
			ef.insert(0L);
			ef.insert(198563254512664L);
			ef.insert(152326598598562L);
			
			assertEquals(0L, (long)ef.select(0));
			assertEquals(198563254512664L, (long)ef.select(1));
			assertEquals(152326598598562L, (long)ef.select(2));
			
		}
		finally{
			tx.commit();
		}
		
		tx = efm.beginTransaction();
		try{
			EntityFile<Long> ef = efm.getEntityFile("long", tx, Long.class);
			assertEquals(0L, (long)ef.select(0));
			assertEquals(198563254512664L, (long)ef.select(1));
			assertEquals(152326598598562L, (long)ef.select(2));
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
