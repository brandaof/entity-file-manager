package org.brandao.entityfilemanager;

import java.io.IOException;

import org.brandao.entityfilemanager.tx.EntityFileTransactionUtil;

public class BulkOperations{

	public static <T> int read(long[] ids, T[] values, 
			EntityFileAccess<T, ?, ?> efa, int off, int len) throws IOException{
		return read(ids, values, efa, off, len, false);
	}

	public static <R> int readRaw(long[] ids, R[] values, 
			EntityFileAccess<?, R, ?> efa, int off, int len) throws IOException{
		return read(ids, values, efa, off, len, true);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static int read(long[] ids, Object[] values, 
			EntityFileAccess efa, int off, int len, boolean raw) throws IOException{
		
		int read = 0;
		int r    = -1;
		int last = off + len;

		int nextOff;
		int q;
		
		while(off < last){
			
			q = EntityFileTransactionUtil.getLenNextSequenceGroup(ids, off);
			nextOff = off + q;
			
			efa.seek(ids[off]);
			r      = raw? efa.readRaw(values, off, q) : efa.read(values, off, q);
			read   += r;
			
			off = nextOff;
			
		}
		
		return read;
		
	}
	
}
