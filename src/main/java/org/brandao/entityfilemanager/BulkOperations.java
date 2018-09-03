package org.brandao.entityfilemanager;

import java.io.IOException;
import java.util.concurrent.locks.Lock;

import org.brandao.entityfilemanager.tx.EntityFileTransactionUtil;

public class BulkOperations{

	public static <T> int read(long[] ids, T[] values, 
			EntityFileAccess<T, ?, ?> efa, int off, int len) throws IOException{
		return read(ids, values, efa, off, len, false, false);
	}

	public static <R> int readRaw(long[] ids, R[] values, 
			EntityFileAccess<?, R, ?> efa, int off, int len) throws IOException{
		return read(ids, values, efa, off, len, true, false);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static int read(long[] ids, Object[] values, 
			EntityFileAccess efa, int off, int len, boolean raw, boolean lock) throws IOException{
		
		int read = 0;
		int r    = -1;
		int last = off + len;

		int q;
		
		while(off < last){
			
			q = EntityFileTransactionUtil.getLenNextSequenceGroup(ids, off);
			
			Lock l = efa.getLock();
			if(lock){
				l.lock();
			}
			try{
				efa.seek(ids[off]);
				r      = raw? efa.readRaw(values, off, q) : efa.read(values, off, q);
			}
			finally{
				if(lock){
					l.unlock();
				}
			}
			
			read   += r;
			
			off += q;
			
		}
		
		return read;
		
	}

	public static <T> void write(long[] ids, T[] values, 
			EntityFileAccess<T, ?, ?> efa, int off, int len) throws IOException{
		write(ids, values, efa, off, len, false, false);
	}

	public static <R> void writeRaw(long[] ids, R[] values, 
			EntityFileAccess<?, R, ?> efa, int off, int len) throws IOException{
		write(ids, values, efa, off, len, true, false);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static void write(long[] ids, Object[] values, 
			EntityFileAccess efa, int off, int len, boolean raw, boolean lock) throws IOException{
		
		int last = off + len;
		int q;
		
		while(off < last){
			
			q = EntityFileTransactionUtil.getLenNextSequenceGroup(ids, off);
			
			Lock l = efa.getLock();
			if(lock){
				l.lock();
			}
			try{
				
				efa.seek(ids[off]);
				if(raw){
					efa.writeRaw(values, off, q);
				}
				else{
					efa.write(values, off, q);				
				}
				
			}
			finally{
				if(lock){
					l.unlock();
				}
			}
			
			off += off + q;
			
		}
		
	}
	
}
