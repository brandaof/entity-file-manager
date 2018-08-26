package org.brandao.entityfilemanager.tx;

import java.util.Map;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.LockProvider;
import org.brandao.entityfilemanager.tx.readcommited.ReadCommitedTransactionalEntityFile;

public class ReadCommitedEntityFileTransaction 
	extends AbstractEntityFileTransaction{

	public ReadCommitedEntityFileTransaction(
			EntityFileTransactionManagerConfigurer entityFileTransactionManager,
			LockProvider lockProvider,
			Map<EntityFileAccess<?, ?, ?>, TransactionEntity<?, ?>> transactionFiles,
			byte status, long transactionID, boolean started,
			boolean rolledBack, boolean commited, long timeout) {
		super(entityFileTransactionManager, lockProvider, transactionFiles, status,
				transactionID, EntityFileTransaction.TRANSACTION_READ_COMMITED, 
				started, rolledBack, commited, timeout);
	}
	
	protected <T,R, H> TransactionEntity<T,R> createTransactionalEntityFile(
			EntityFileAccess<T,R,H> entityFile, long transactionID,	byte transactionIsolation,
			EntityFileTransactionManagerConfigurer entityFileTransactionManagerConfigurer){
		
		TransientTransactionEntityFileAccess<T, R, H> txFile = 
				new TransientTransactionEntityFileAccess<T, R, H>(
						entityFile, null, transactionID, transactionIsolation);
		
		return 
			new ReadCommitedTransactionalEntityFile<T, R, H>(
					txFile, this.lockProvider, this.timeout);
	}

}
