package org.brandao.entityfilemanager.tx;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.brandao.entityfilemanager.EntityFileAccess;

public class EntityFileTransactionUtil {

	public static final byte OP_TYPE_FILTER = Byte.valueOf("00000111", 2);

	public static final String EXTENSION = "txa";
	
	@SuppressWarnings("serial")
	private static Map<Integer,Byte> mappedTransactionStatus = new HashMap<Integer,Byte>(){{
		
		//transaction not started only
		put(~EntityFileTransaction.TRANSACTION_NOT_STARTED,
				EntityFileTransaction.TRANSACTION_NOT_STARTED);
		
		//transaction started rollback only
		put(~EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK, 
				EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK);
		
		//transaction rolledback only
		put(~EntityFileTransaction.TRANSACTION_ROLLEDBACK, 
				EntityFileTransaction.TRANSACTION_ROLLEDBACK);
		
		//transaction started commit only
		put(~EntityFileTransaction.TRANSACTION_STARTED_COMMIT, 
				EntityFileTransaction.TRANSACTION_STARTED_COMMIT);

		//transaction commited only
		put(~EntityFileTransaction.TRANSACTION_COMMITED, 
				EntityFileTransaction.TRANSACTION_COMMITED);
	
		//transaction started and not started
		put(~(EntityFileTransaction.TRANSACTION_NOT_STARTED | EntityFileTransaction.TRANSACTION_STARTED_COMMIT), 
				EntityFileTransaction.TRANSACTION_NOT_STARTED);
		
		//transaction started with rollback
		put(~(EntityFileTransaction.TRANSACTION_NOT_STARTED | EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK), 
				EntityFileTransaction.TRANSACTION_NOT_STARTED);
		
		//transaction started rollback without fail
		put(~(EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK | EntityFileTransaction.TRANSACTION_ROLLEDBACK), 
				EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK);
		
		//transaction started commit without fail
		put(~(EntityFileTransaction.TRANSACTION_STARTED_COMMIT | EntityFileTransaction.TRANSACTION_COMMITED), 
				EntityFileTransaction.TRANSACTION_STARTED_COMMIT);
		
		//transaction started commit with fail
		put(~(EntityFileTransaction.TRANSACTION_STARTED_COMMIT | EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK), 
				EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK);
		
		put(~(EntityFileTransaction.TRANSACTION_STARTED_COMMIT | EntityFileTransaction.TRANSACTION_ROLLEDBACK), 
				EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK);
		
		put(~(EntityFileTransaction.TRANSACTION_STARTED_COMMIT | EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK | EntityFileTransaction.TRANSACTION_ROLLEDBACK), 
				EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK);
		
		//transaction commited with fail
		put(~(EntityFileTransaction.TRANSACTION_COMMITED | EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK), 
				EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK);
		
		put(~(EntityFileTransaction.TRANSACTION_COMMITED | EntityFileTransaction.TRANSACTION_ROLLEDBACK), 
				EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK);
		
		put(~(EntityFileTransaction.TRANSACTION_COMMITED | EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK | EntityFileTransaction.TRANSACTION_ROLLEDBACK), 
				EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK);

		//transaction commit with fail
		put(~(EntityFileTransaction.TRANSACTION_COMMITED | EntityFileTransaction.TRANSACTION_STARTED_COMMIT | EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK), 
				EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK);
		
		put(~(EntityFileTransaction.TRANSACTION_COMMITED | EntityFileTransaction.TRANSACTION_STARTED_COMMIT | EntityFileTransaction.TRANSACTION_ROLLEDBACK), 
				EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK);
		
		put(~(EntityFileTransaction.TRANSACTION_COMMITED | EntityFileTransaction.TRANSACTION_STARTED_COMMIT | EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK | EntityFileTransaction.TRANSACTION_ROLLEDBACK), 
				EntityFileTransaction.TRANSACTION_STARTED_ROLLBACK);
		
	}};
	
	@SuppressWarnings("rawtypes")
	private static final RawTransactionEntity[] EMPTY_ARRAY = new RawTransactionEntity[0];
	
	@SuppressWarnings("unchecked")
	public static <R> RawTransactionEntity<R>[][] mapOperations(
			RawTransactionEntity<R>[] ops){
		
		int startSize = (int)(ops.length*0.5);
		startSize     = startSize < 10? 10 : startSize;
		
		RawTransactionEntity<R>[][] result = new RawTransactionEntity[OP_TYPE_FILTER][startSize];
		int[] count                        = new int[OP_TYPE_FILTER];
		
		for(RawTransactionEntity<R> op: ops){
			
			int opType                  = op.getFlags() & OP_TYPE_FILTER;
			int c                       = count[opType];
			RawTransactionEntity<R>[] a = result[opType]; 
			
			if(c == a.length){
				RawTransactionEntity<R>[] tmp = new RawTransactionEntity[a.length + startSize];
				
				System.arraycopy(a, 0, tmp, 0, a.length);
				
				a              = tmp;
				result[opType] = tmp;
			}
			
			a[c]          = op;
			count[opType] = c++;
		}
		
		result[TransactionalEntity.NEW_RECORD] = 
			adjustArray(
				result[TransactionalEntity.NEW_RECORD], 
				count[TransactionalEntity.NEW_RECORD]);
		
		result[TransactionalEntity.UPDATE_RECORD] = 
			adjustArray(
				result[TransactionalEntity.UPDATE_RECORD], 
				count[TransactionalEntity.UPDATE_RECORD]);
				
		result[TransactionalEntity.DELETE_RECORD] = 
			adjustArray(
				result[TransactionalEntity.DELETE_RECORD], 
				count[TransactionalEntity.DELETE_RECORD]);
		
		return result;
	}
	
	public static File getTransactionFile(File file, long transactionID){
		String name = file.getName();
		String[] parts = name.split("\\.");
		return new File(
			file.getParentFile(), 
			parts[0] + "-" + Long.toString(transactionID, Character.MAX_RADIX) + ".txa"
		);
	}

	public static TransactionFileNameMetadata getTransactionFileNameMetadata(File file){
		String name = file.getName();
		String[] parts = name.split("\\.");
		String[] nameParts = parts[0].split("\\-");
		return new TransactionFileNameMetadata(nameParts[0], 
				Long.parseLong(parts[1], Character.MAX_RADIX));
	}
	
	public static <T,R> TransactionEntityFileAccess<T,R> getTransactionEntityFileAccess( 
		EntityFileAccess<T,R> entityFile, long transactionID, byte transactionIsolation) {
		return new TransactionEntityFileAccess<T,R>(entityFile, transactionID, transactionIsolation);
	}
	
	@SuppressWarnings("unchecked")
	private static <R> RawTransactionEntity<R>[] adjustArray(RawTransactionEntity<R>[] value, int len){
		
		if(len == 0){
			return EMPTY_ARRAY;
		}
		
		RawTransactionEntity<R>[] result = new RawTransactionEntity[len];
		System.arraycopy(value, 0, result, 0, len);
		return result;
	}
	
	public static class TransactionFileNameMetadata{
		
		private String name;
		
		private long transactionID;

		public TransactionFileNameMetadata(String name, long transactionID) {
			super();
			this.name = name;
			this.transactionID = transactionID;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public long getTransactionID() {
			return transactionID;
		}

		public void setTransactionID(long transactionID) {
			this.transactionID = transactionID;
		}
		
	}
	
	public static byte mergeTransactionStatus(
			Map<EntityFileAccess<?,?>, TransactionalEntityFile<?,?>> map) throws IOException{
		int result = 0;
		
		for(TransactionalEntityFile<?,?> txFile: map.values()){
			result = result | txFile.getTransactionStatus();
		}
		
		return (byte)result;
	}
	
	public static byte getTransactionStatus(byte mergedTransactionStatus){
		
		for(Entry<Integer,Byte> entry: mappedTransactionStatus.entrySet()){
			if((entry.getKey() & mergedTransactionStatus) == mergedTransactionStatus){
				return entry.getValue();
			}
		}
		
		return 0;
	}
	
	public static long[] getNextSequenceGroup(long[] ids, int off){
		
		int max = ids.length;
		
		if(off >= max || ids[off + 1] != (ids[off] + 1)){
			return null;
		}
		
		int end = off + 2;
		
		while(end < max && ids[end] == (ids[end++ - 1] - 1));
		
		int len = end - off;
		long[] result = new long[len];
		System.arraycopy(ids, off, result, 0, len);
		return result;
	}

	public static <T> Map<Long,Integer> getMappedIdIndex(long[] ids){
		
		Map<Long, Integer> result = new HashMap<Long, Integer>();
		
		for(int i=0;i<ids.length;i++){
			result.put(ids[i], i);
		}
		
		return result;
	}

}
