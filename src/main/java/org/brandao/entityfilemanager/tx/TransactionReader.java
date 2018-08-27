package org.brandao.entityfilemanager.tx;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.EntityFileManagerConfigurer;
import org.brandao.entityfilemanager.FileAccess;

public class TransactionReader {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public ConfigurableEntityFileTransaction read(
			EntityFileTransactionManagerConfigurer entityFileTransactionManagerConfigurer,
			RandomAccessFile transactionFile) throws IOException, TransactionException{

		EntityFileManagerConfigurer entityFileManagerConfigurer = 
				entityFileTransactionManagerConfigurer.getEntityFileManagerConfigurer();
		
		FileAccess fa = new FileAccess(null, transactionFile);
		
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
					entityFileTransactionManagerConfigurer
						.createTransactionEntityFileAccess(efa, transactionID, transactionIsolation);
			
			SubtransactionEntityFileAccess<?,?,?> stf = 
					new SubtransactionEntityFileAccess(
							fa.getFilePointer(),
							transactionFile,
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
		
		return entityFileTransactionManagerConfigurer
				.load(m, status, transactionID, transactionIsolation, 
						started, rolledBack, commited, timeout);
	}
	
}
