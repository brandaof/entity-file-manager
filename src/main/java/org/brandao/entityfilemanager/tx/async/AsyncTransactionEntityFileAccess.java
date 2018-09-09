package org.brandao.entityfilemanager.tx.async;

import java.io.File;
import java.io.IOException;

import org.brandao.entityfilemanager.tx.TransientTransactionEntityFileAccess;

public class AsyncTransactionEntityFileAccess<T, R, H> 
	extends TransientTransactionEntityFileAccess<T, R, H>{

	private AsyncAutoFlushVirutalEntityFileAccess<T,R,H> parent;
	
	private boolean registered;
	
	public AsyncTransactionEntityFileAccess(
			AsyncAutoFlushVirutalEntityFileAccess<T,R,H> e,
			File file, long transactionID, byte transactionIsolation) {
		super(e, file, transactionID, transactionIsolation);
		this.registered = false;
		this.parent = e;
	}

	public void createNewFile() throws IOException{
		super.createNewFile();
		parent.register();
		registered = true;
	}
	
	public void close() throws IOException{
		try{
			super.close();
		}
		finally{
			if(registered){
				parent.unregister();
			}
		}
	}
}
