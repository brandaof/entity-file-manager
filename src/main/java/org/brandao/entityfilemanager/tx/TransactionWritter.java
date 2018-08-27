package org.brandao.entityfilemanager.tx;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.Map;

import org.brandao.entityfilemanager.DataOutputStream;
import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.FileAccess;

public class TransactionWritter {

	public void write(ConfigurableEntityFileTransaction t, RandomAccessFile transactionFile) throws IOException{
		
		FileAccess fa = new FileAccess(null, transactionFile);
		
		ByteArrayOutputStream bout = new ByteArrayOutputStream(18);
		DataOutputStream dout      = new DataOutputStream(bout);
		
		dout.writeByte(t.getStatus());
		dout.writeLong(t.getTimeout());
		dout.writeLong(t.getTransactionID());
		
		dout.writeByte((byte)(
				(t.isClosed()?      1 : 0) |
				(t.isCommited()?    2 : 0) |
				(t.isDirty()?       4 : 0) |
				(t.isRolledBack()?  8 : 0) |
				(t.isStarted()?    16 : 0)));
		
		fa.write(bout.toByteArray());
		
		Map<EntityFileAccess<?,?,?>, TransactionEntity<?,?>> m = t.getTransactionFiles();
		
		for(TransactionEntity<?,?> tt: m.values()){
			SubtransactionEntityFileAccess<?,?,?> stf = 
					new SubtransactionEntityFileAccess(
							fa.getFilePointer(),
							tt.getEntityFileAccess().length(), 
							transactionFile,
							tt.getEntityFileAccess());
		}
		
	}
	
}
