package org.brandao.entityfilemanager.tx;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.EntityFileManagerConfigurer;
import org.brandao.entityfilemanager.FileAccess;

public class TransactionReader {

	public ConfigurableEntityFileTransaction read(
			EntityFileTransactionManagerConfigurer eftmc, File f) throws IOException, TransactionException{
		RandomAccessFile tf = new RandomAccessFile(f, "rw");
		try{
			return this.read(eftmc, tf, f);
		}
		finally{
			tf.close();
		}
		
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private ConfigurableEntityFileTransaction read(
			EntityFileTransactionManagerConfigurer eftmc,
			RandomAccessFile tf, File f) throws IOException, TransactionException{

		EntityFileManagerConfigurer entityFileManagerConfigurer = 
				eftmc.getEntityFileManagerConfigurer();
		
		FileAccess fa = new FileAccess(f, tf);
		
		byte status               = fa.readByte();
		long timeout              = fa.readLong();
		long transactionID        = fa.readLong();
		byte transactionIsolation = fa.readByte();
		
		byte flags = fa.readByte();
		
		boolean commited   = (flags & 1) != 0;
		boolean rolledBack = (flags & 2) != 0;
		boolean started    = (flags & 4) != 0;
		
		Map<EntityFileAccess<?,?,?>, TransactionEntityFileAccess<?,?,?>> m = 
				new HashMap<EntityFileAccess<?,?,?>, TransactionEntityFileAccess<?,?,?>>();
		
		while(fa.readByte() == 0){
			
			int nameLen = fa.readInt();
			String name = fa.readString(nameLen + 2);
			
			EntityFileAccess<?,?,?> efa = entityFileManagerConfigurer.getEntityFile(name);
			
			if(efa == null){
				throw new TransactionException("entity file access not found: " + name);
			}
			
			TransactionEntityFileAccess tef = 
					eftmc
						.createTransactionEntityFileAccess(efa, transactionID, transactionIsolation);
			
			InnerEntityFileAccess<?,?,?> stf = 
					new InnerEntityFileAccess(
							fa.getFilePointer(),
							fa,
							tef);

			stf.open();
			stf.seek(0);
			
			long count = 0;
			long max   = stf.length();
			long len   = tef.getBatchLength();
			
			while(count < max){
				Object[] tmp = stf.batchRead((int)len);
				tef.batchWrite(tmp);
				count += tmp.length;
			}
			
			m.put(efa, tef);
		}
		
		return eftmc
				.load(m, status, transactionID, transactionIsolation, 
						started, rolledBack, commited, timeout);
	}
	
}
