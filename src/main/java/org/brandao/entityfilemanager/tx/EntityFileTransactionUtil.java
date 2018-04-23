package org.brandao.entityfilemanager.tx;

import java.io.File;

public class EntityFileTransactionUtil {

	public static final byte OP_TYPE_FILTER = Byte.valueOf("00000111", 2);

	public static final String EXTENSION = "txa";
	
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
}
