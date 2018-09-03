package org.brandao.entityfilemanager.tx.async;

import java.io.File;
import java.io.IOException;

import org.brandao.entityfilemanager.LongEntityFileAccess;
import org.brandao.entityfilemanager.LongEntityFileAccess.LongEntityFileAccessHeader;

import junit.framework.TestCase;

public class AutoFlushVirutalEntityFileAccessTest 
	extends TestCase {
	
	public void testNew() throws IOException{
		LongEntityFileAccess efa = new LongEntityFileAccess("long", new File("./test/long"));
		AutoFlushVirutalEntityFileAccess<Long, byte[], LongEntityFileAccessHeader> vefa = 
				new AutoFlushVirutalEntityFileAccess<Long, byte[], LongEntityFileAccessHeader>(efa);

		efa.createNewFile();
		vefa.createNewFile();
		
		try{
			assertEquals(0L, vefa.length());
			assertEquals(0L, efa.length());
		}
		finally{
			efa.close();
			efa.delete();
			vefa.close();
			vefa.delete();
		}
	}

	public void testNewSync() throws IOException{
		LongEntityFileAccess efa = new LongEntityFileAccess("long", new File("./test/long"));
		efa.createNewFile();
		efa.seek(0);
		efa.write(152632L);
		
		AutoFlushVirutalEntityFileAccess<Long, byte[], LongEntityFileAccessHeader> vefa = 
				new AutoFlushVirutalEntityFileAccess<Long, byte[], LongEntityFileAccessHeader>(efa);

		vefa.createNewFile();
		
		try{
			assertEquals(1L, vefa.length());
			assertEquals(1L, efa.length());
			
			vefa.seek(0);
			assertEquals(152632L, vefa.read().longValue());
			
		}
		finally{
			efa.close();
			efa.delete();
			vefa.close();
			vefa.delete();
		}
	}

	public void testInsertVirtual() throws IOException{
		LongEntityFileAccess efa = new LongEntityFileAccess("long", new File("./test/long"));
		efa.createNewFile();
		
		AutoFlushVirutalEntityFileAccess<Long, byte[], LongEntityFileAccessHeader> vefa = 
				new AutoFlushVirutalEntityFileAccess<Long, byte[], LongEntityFileAccessHeader>(efa);

		vefa.createNewFile();
		
		try{
			assertEquals(0L, vefa.length());
			assertEquals(0L, efa.length());

			vefa.seek(0);
			vefa.write(152632L);

			assertEquals(1L, vefa.length());
			assertEquals(0L, efa.length());

			vefa.seek(0);
			assertEquals(152632L, vefa.read().longValue());
			
		}
		finally{
			efa.close();
			efa.delete();
			vefa.close();
			vefa.delete();
		}
	}

	public void testUpdateVirtual() throws IOException{
		LongEntityFileAccess efa = new LongEntityFileAccess("long", new File("./test/long"));
		efa.createNewFile();
		
		AutoFlushVirutalEntityFileAccess<Long, byte[], LongEntityFileAccessHeader> vefa = 
				new AutoFlushVirutalEntityFileAccess<Long, byte[], LongEntityFileAccessHeader>(efa);

		vefa.createNewFile();
		
		try{
			assertEquals(0L, vefa.length());
			assertEquals(0L, efa.length());
			assertEquals(0L, vefa.getVirtual().length());

			vefa.seek(0);
			vefa.write(152632L);
			vefa.write(12L);

			vefa.seek(0);
			assertEquals(152632L, vefa.read().longValue());
			assertEquals(12L,     vefa.read().longValue());
			assertEquals(2L,      vefa.length());
			assertEquals(0L,      efa.length());
			assertEquals(2L,      vefa.getVirtual().length());

			vefa.seek(0);
			vefa.write(127L);

			vefa.seek(0);
			assertEquals(127L, vefa.read().longValue());
			assertEquals(12L,  vefa.read().longValue());
			assertEquals(2L,   vefa.length());
			assertEquals(0L,   efa.length());
			assertEquals(3L,   vefa.getVirtual().length());
			
			vefa.seek(0);
			vefa.write(13L);
			vefa.write(12L);

			vefa.seek(0);
			assertEquals(13L, vefa.read().longValue());
			assertEquals(12L, vefa.read().longValue());
			assertEquals(2L,  vefa.length());
			assertEquals(0L,  efa.length());
			assertEquals(5L,  vefa.getVirtual().length());
			
			vefa.getVirtual().seek(0);
			assertEquals(152632L, vefa.getVirtual().read().longValue());
			assertEquals(12L,     vefa.getVirtual().read().longValue());
			assertEquals(127L,    vefa.getVirtual().read().longValue());
			assertEquals(13L,     vefa.getVirtual().read().longValue());
			assertEquals(12L,     vefa.getVirtual().read().longValue());
		}
		finally{
			efa.close();
			efa.delete();
			vefa.close();
			vefa.delete();
		}
	}

	public void testBulkInsertVirtual() throws IOException{
		LongEntityFileAccess efa = new LongEntityFileAccess("long", new File("./test/long"));
		efa.createNewFile();
		
		AutoFlushVirutalEntityFileAccess<Long, byte[], LongEntityFileAccessHeader> vefa = 
				new AutoFlushVirutalEntityFileAccess<Long, byte[], LongEntityFileAccessHeader>(efa);

		vefa.createNewFile();
		
		try{
			assertEquals(0L, vefa.length());
			assertEquals(0L, efa.length());
			assertEquals(0L, vefa.getVirtual().length());

			vefa.seek(0);
			vefa.batchWrite(new Long[]{152632L, 12L});

			vefa.seek(0);
			assertEquals(152632L, vefa.read().longValue());
			assertEquals(12L,     vefa.read().longValue());
			assertEquals(2L,      vefa.length());
			assertEquals(0L,      efa.length());
			assertEquals(2L,      vefa.getVirtual().length());

			vefa.seek(0);
			vefa.batchWrite(new Long[]{127L});

			vefa.seek(0);
			assertEquals(127L, vefa.read().longValue());
			assertEquals(12L,  vefa.read().longValue());
			assertEquals(2L,   vefa.length());
			assertEquals(0L,   efa.length());
			assertEquals(3L,   vefa.getVirtual().length());
			
			vefa.seek(0);
			vefa.batchWrite(new Long[]{13L, 12L});

			vefa.seek(0);
			assertEquals(13L, vefa.read().longValue());
			assertEquals(12L, vefa.read().longValue());
			assertEquals(2L,  vefa.length());
			assertEquals(0L,  efa.length());
			assertEquals(5L,  vefa.getVirtual().length());
			
			vefa.getVirtual().seek(0);
			assertEquals(152632L, vefa.getVirtual().read().longValue());
			assertEquals(12L,     vefa.getVirtual().read().longValue());
			assertEquals(127L,    vefa.getVirtual().read().longValue());
			assertEquals(13L,     vefa.getVirtual().read().longValue());
			assertEquals(12L,     vefa.getVirtual().read().longValue());
		}
		finally{
			efa.close();
			efa.delete();
			vefa.close();
			vefa.delete();
		}
	}
	
}
