package org.brandao.entityfilemanager.tx;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collection;
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
		
		boolean closed     = (flags &  1) != 0;
		boolean commited   = (flags &  2) != 0;
		boolean dirty      = (flags &  4) != 0;
		boolean rolledBack = (flags &  8) != 0;
		boolean started    = (flags & 16) != 0;
		
		Map<EntityFileAccess<?,?,?>, TransactionEntityFileAccess<?,?,?>> m = 
				new HashMap<EntityFileAccess<?,?,?>, TransactionEntityFileAccess<?,?,?>>();
		
		Collection<TransactionEntityFileAccess<?,?,?>> list = m.values();
		
		while(fa.readByte() == 0){
			
			int nameLen = fa.readInt();
			String name = fa.readString(nameLen + 2);
			
			EntityFileAccess<?,?,?> efa = entityFileManagerConfigurer.getEntityFile(name);
			
			if(efa == null){
				throw new TransactionException("entity file access not found: " + name);
			}
			
			SubtransactionEntityFileAccess<?,?,?> stf = 
					new SubtransactionEntityFileAccess(
							fa.getFilePointer(),
							transactionFile,
							efa);
			
			stf.open();
			
			m.put(efa, stf);
		}
	}
	
}
