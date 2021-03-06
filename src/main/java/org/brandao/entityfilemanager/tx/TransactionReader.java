package org.brandao.entityfilemanager.tx;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.EntityFileManagerConfigurer;
import org.brandao.entityfilemanager.FileAccess;

public class TransactionReader {

	@SuppressWarnings({ "rawtypes", "unchecked", "unused" })
	public ConfigurableEntityFileTransaction read(
			EntityFileTransactionManagerConfigurer eftmc,
			FileAccess fa) throws IOException, TransactionException{

		EntityFileManagerConfigurer entityFileManagerConfigurer = 
				eftmc.getEntityFileManagerConfigurer();
		
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
					eftmc.getEntityFileTransactionFactory()
						.createTransactionEntityFileAccess(efa, transactionID, transactionIsolation);
			
			InnerEntityFileAccess<?,?,?> stf = 
					new InnerEntityFileAccess(
							fa.getFilePointer(),
							fa,
							tef);

			stf.open();
			stf.seek(0);
			
			long count   = 0;
			long max     = stf.length();
			long batch   = stf.getBatchLength();
			long maxRead = batch > max? max : batch;
			
			while(count < max){
				long avail = max - count;
				long read  = avail > maxRead? maxRead : avail;
				
				Object[] tmp = stf.batchRead((int)read);
				tef.batchWrite(tmp);
				
				count += tmp.length;
			}
			
			m.put(efa, tef);
			
			stf.close();
		}
		
		return eftmc
				.load(m, status, transactionID, transactionIsolation, 
						false, rolledBack, commited, timeout);
	}
	
}
