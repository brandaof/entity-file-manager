package org.brandao.entityfilemanager.tx;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collection;
import java.util.Map;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.FileAccess;

public class TransactionWritter {

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void write(ConfigurableEntityFileTransaction t, RandomAccessFile transactionFile) throws IOException{

		FileAccess fa = new FileAccess(null, transactionFile);
		
		fa.writeByte(t.getStatus());
		fa.writeLong(t.getTimeout());
		fa.writeLong(t.getTransactionID());
		fa.writeByte(t.getTransactionIsolation());
		
		fa.writeByte((byte)(
				(t.isCommited()?   1 : 0) |
				(t.isRolledBack()? 2 : 0) |
				(t.isStarted()?    4 : 0)));
		
		
		Map<EntityFileAccess<?,?,?>, TransactionEntity<?,?>> m = t.getTransactionFiles();
		Collection<TransactionEntity<?,?>> list = m.values();
		
		for(TransactionEntity<?,?> tt: list){
			
			fa.writeByte((byte)0);
			
			TransactionEntityFileAccess tefa = tt.getTransactionEntityFileAccess();
			
			String name = tefa.getName();
			fa.writeInt(name.length());
			fa.writeString(name, name.length() + 2);
			
			SubtransactionEntityFileAccess stf = 
					new SubtransactionEntityFileAccess(
							fa.getFilePointer(),
							transactionFile,
							tefa);
			
			long count = 0;
			long max   = tefa.length();
			long len   = tefa.getBatchLength();
			
			stf.createNewFile();
			
			tefa.seek(0);
			
			while(count < max){
				Object[] tmp = tefa.batchRead((int)len);
				stf.batchWrite(tmp);
				count += tmp.length;
			}
			
		}

		fa.writeByte((byte)0xff);
		
	}
	
}
