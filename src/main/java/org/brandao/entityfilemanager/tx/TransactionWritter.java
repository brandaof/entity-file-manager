package org.brandao.entityfilemanager.tx;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collection;
import java.util.Map;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.FileAccess;

public class TransactionWritter {

	
	public void write(ConfigurableEntityFileTransaction ceft, File f) throws IOException{
		RandomAccessFile tf = new RandomAccessFile(f, "rw");
		try{
			this.write(ceft, tf, f);
		}
		finally{
			tf.close();
		}
		
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void write(ConfigurableEntityFileTransaction ceft, 
			RandomAccessFile tf, File f) throws IOException{

		FileAccess fa = new FileAccess(f, tf);
		
		fa.writeByte(ceft.getStatus());
		fa.writeLong(ceft.getTimeout());
		fa.writeLong(ceft.getTransactionID());
		fa.writeByte(ceft.getTransactionIsolation());
		
		fa.writeByte((byte)(
				(ceft.isCommited()?   1 : 0) |
				(ceft.isRolledBack()? 2 : 0) |
				(ceft.isStarted()?    4 : 0)));
		
		
		Map<EntityFileAccess<?,?,?>, TransactionEntity<?,?>> m = ceft.getTransactionFiles();
		Collection<TransactionEntity<?,?>> list = m.values();
		
		for(TransactionEntity<?,?> tt: list){
			
			fa.writeByte((byte)0);
			
			TransactionEntityFileAccess tefa = tt.getTransactionEntityFileAccess();
			
			String name = tefa.getName();
			fa.writeInt(name.length());
			fa.writeString(name, name.length() + 2);
			
			InnerEntityFileAccess stf = 
					new InnerEntityFileAccess(
							fa.getFilePointer(),
							fa,
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
		
		fa.flush();
	}
	
}
