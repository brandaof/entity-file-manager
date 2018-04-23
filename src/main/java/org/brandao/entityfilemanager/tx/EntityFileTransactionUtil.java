package org.brandao.entityfilemanager.tx;

public class EntityFileTransactionUtil {

	public static final byte OP_TYPE_FILTER = Byte.valueOf("00000111", 2);

	@SuppressWarnings("rawtypes")
	private static final RawTransactionEntity[] EMPTY_ARRAY = new RawTransactionEntity[0];
	
	@SuppressWarnings("unchecked")
	public static <R> RawTransactionEntity<R>[][] mapOperations(
			RawTransactionEntity<R>[] ops){
		
		RawTransactionEntity<R>[][] result	= new RawTransactionEntity[OP_TYPE_FILTER][ops.length];
		int[] count 						= new int[OP_TYPE_FILTER];
		
		for(RawTransactionEntity<R> op: ops){
			int opType = op.getFlags() & OP_TYPE_FILTER;
			result[opType][count[opType]] = op;
			count[opType]++;
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
	
	@SuppressWarnings("unchecked")
	private static <R> RawTransactionEntity<R>[] adjustArray(RawTransactionEntity<R>[] value, int len){
		
		if(len == 0){
			return EMPTY_ARRAY;
		}
		
		RawTransactionEntity<R>[] result = new RawTransactionEntity[len];
		System.arraycopy(value, 0, result, 0, len);
		return result;
	}
	
}
