package org.brandao.entityfilemanager.tx;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Comparator;
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
	
	public static int[][] mapOperations(long[] ids, 
			Map<Long,Long> map, int off, int len){
		
		int[][] result  = new int[OP_TYPE_FILTER][10];
		int[] count     = new int[OP_TYPE_FILTER];
		
		int opType;
		int c;
		int[] a; 
		int[] tmp;
		
		int max = off + len; 
		
		for(int i=off;i<max;i++){
			opType = (map.containsKey(ids[i])? TransactionalEntity.UPDATE_RECORD : TransactionalEntity.NEW_RECORD) & OP_TYPE_FILTER;
			c      = count[opType];
			a      = result[opType]; 
			
			if(c == a.length){
				tmp = new int[a.length*2];
				System.arraycopy(a, 0, tmp, 0, a.length);
				a              = tmp;
				result[opType] = tmp;
			}
			
			a[c]          = i;
			count[opType] = ++c;
		}
		
		for(int i=0;i<OP_TYPE_FILTER;i++){
			result[i] = adjustArray(result[i], count[i]);
		}
		
		return result;
	}
	
	@SuppressWarnings("unchecked")
	public static <R> RawTransactionEntity<R>[][] mapOperations(
			RawTransactionEntity<R>[] ops){
		
		Arrays.sort(ops);
		
		RawTransactionEntity<R>[][] result = new RawTransactionEntity[OP_TYPE_FILTER][10];
		int[] count                        = new int[OP_TYPE_FILTER];
		
		for(RawTransactionEntity<R> op: ops){
			
			int opType                  = op.getFlags() & OP_TYPE_FILTER;
			int c                       = count[opType];
			RawTransactionEntity<R>[] a = result[opType]; 
			
			if(c == a.length){
				RawTransactionEntity<R>[] tmp = new RawTransactionEntity[a.length*2];
				System.arraycopy(a, 0, tmp, 0, a.length);
				a              = tmp;
				result[opType] = tmp;
			}
			
			a[c]          = op;
			count[opType] = ++c;
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

	public static <T,R,H> TransactionEntityFileAccess<T,R,H> getTransactionEntityFileAccess( 
		EntityFileAccess<T,R,H> entityFile, long transactionID, byte transactionIsolation, 
		EntityFileTransactionManagerConfigurer entityFileTransactionManagerConfigurer) {
		File transactionFile = 
				getTransactionFile(
						entityFile.getAbsoluteFile(), 
						transactionID, 
						entityFileTransactionManagerConfigurer);
		return new TransactionEntityFileAccess<T,R,H>(entityFile, transactionFile, transactionID, transactionIsolation);
	}
	
	
	public static File getTransactionFile(File file, long transactionID, 
			EntityFileTransactionManagerConfigurer entityFileTransactionManagerConfigurer){
		String name = file.getName();
		String[] parts = name.split("\\.");
		return new File(
				entityFileTransactionManagerConfigurer.getTransactionPath(), 
			parts[0] + "-" + Long.toString(transactionID, Character.MAX_RADIX) + ".txa"
		);
	}

	public static TransactionFileNameMetadata getTransactionFileNameMetadata(File file){
		String name = file.getName();
		String[] parts = name.split("\\.");
		String[] nameParts = parts[0].split("\\-");
		return new TransactionFileNameMetadata(nameParts[0], 
				Long.parseLong(parts[1], Character.MAX_RADIX), file);
	}

	public static <T,R,H> TransactionEntityFileAccess<T,R,H> getTransactionEntityFileAccess( 
		EntityFileAccess<T,R,H> entityFile, TransactionFileNameMetadata tfmd) {
		return new TransactionEntityFileAccess<T,R,H>(entityFile, tfmd.getFile(), (byte)-1, (byte)-1);
	}
	
	@SuppressWarnings("unchecked")
	private static <R> R[] adjustArray(R[] value, int len){
		
		if(len == 0){
			return (R[])Array.newInstance(value.getClass().getComponentType(), 0);
		}
		
		R[] result = (R[])Array.newInstance(value.getClass().getComponentType(), len);
		System.arraycopy(value, 0, result, 0, len);
		return result;
	}

	private static int[] adjustArray(int[] value, int len){
		
		if(len == 0){
			return new int[0];
		}
		
		int[] result = new int[len];
		System.arraycopy(value, 0, result, 0, len);
		return result;
	}
	
/*
 	@SuppressWarnings("unchecked")
	private static <R> RawTransactionEntity<R>[] adjustArray(RawTransactionEntity<R>[] value, int len){
		
		if(len == 0){
			return EMPTY_ARRAY;
		}
		
		RawTransactionEntity<R>[] result = new RawTransactionEntity[len];
		System.arraycopy(value, 0, result, 0, len);
		return result;
	}
 */	
	public static byte mergeTransactionStatus(
			Map<EntityFileAccess<?,?,?>, TransactionEntityFileAccess<?,?,?>> map) throws IOException{
		int result = 0;
		
		for(TransactionEntityFileAccess<?,?,?> txFile: map.values()){
			result = result | txFile.getTransactionStatus();
		}
		
		return (byte)result;
	}

	public static byte getTransactionIsolation(
			Map<EntityFileAccess<?,?,?>, TransactionEntityFileAccess<?,?,?>> map
			) throws TransactionException, IOException{
		
		byte result = -1;
		
		for(TransactionEntityFileAccess<?,?,?> txFile: map.values()){
			if(result == -1){
				result = txFile.getTransactionIsolation();
			}
			else
			if(result != txFile.getTransactionIsolation()){
				throw new TransactionException("invalid transaction isolation: expected " + result + " found " + txFile.getTransactionIsolation()); 
			}
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
	
	public static int getLastSequence(long[] ids, int off){
		
		int max = ids.length;

		if(off >= max){
			return off;
		}
		
		if(ids[off] + 1 != ids[++off]){
			return off;
		}
		
		max--;
		
		while(off < max && (ids[off] + 1) == ids[++off]);
		
		return off + 1;
	}

	@Deprecated
	public static <T> Map<Long,Integer> getMappedIdIndex(long[] ids){
		
		Map<Long, Integer> result = new HashMap<Long, Integer>();
		
		for(int i=0;i<ids.length;i++){
			result.put(ids[i], i);
		}
		
		return result;
	}

	@SuppressWarnings("unchecked")
	public static <T> void sort(long[] ids, T[] values){
		
		OrderEntityById<T>[] tmp = new OrderEntityById[ids.length];
		
		for(int i=0;i<ids.length;i++){
			tmp[i] = new OrderEntityById<T>(ids[i], values[i]);
		}
		
		Arrays.sort(tmp);
		
		for(int i=0;i<ids.length;i++){
			OrderEntityById<T> e = tmp[i];
			ids[i]    = e.id;
			values[i] = e.value;
		}
		
	}

	public static long[] refToId(long[] ids, int[] refs){
		long[] result = new long[refs.length];
		for(int i=0;i<result.length;i++){
			result[i] = ids[refs[i]];
		}
		return result;
	}

	public static long[] getTXId(long[] ids, Map<Long,Long> pointerMap){
		long[] result = new long[ids.length];
		for(int i=0;i<result.length;i++){
			result[i] = pointerMap.get(ids[i]);
		}
		return result;
	}
	
	private static class OrderEntityById<T> implements Comparator<OrderEntityById<T>>{
		
		public long id;
		
		public T value;

		public OrderEntityById(long id, T value) {
			super();
			this.id = id;
			this.value = value;
		}

		public int compare(OrderEntityById<T> o1, OrderEntityById<T> o2) {
			return (int)(o1.id - o2.id);
		}
		
	}
	
}
