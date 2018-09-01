package org.brandao.entityfilemanager;

import java.io.IOException;

import org.brandao.entityfilemanager.tx.EntityFileTransactionUtil;

public abstract class ReadBulkArray<T> {

	public T[] read(long[] ids) throws IOException{
		
		T[] result = createArray(ids.length);
		int off    = 0;
		int nextOff;
		int q;
		
		while(off < ids.length){
			q = EntityFileTransactionUtil.getLenNextSequenceGroup(ids, off);
			nextOff = off + q;
			
			if(q == 1){
				result[off] = readItem(ids[off]);
			}
			else{
				long[] subIds   = new long[q];
				System.arraycopy(ids, off, subIds, 0, q);
				
				T[] subEntities = readItens(subIds);
				System.arraycopy(subEntities, 0, result, off, q);
			}
			
			off = nextOff;
		}
		
		return result;
		
	}
	
	protected abstract T[] createArray(int len);
	
	protected abstract T readItem(long id) throws IOException;
	
	protected abstract T[] readItens(long[] ids) throws IOException;
	
	
}
