package org.brandao.entityfilemanager.tx;

import java.util.Map;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.LockProvider;
import org.brandao.entityfilemanager.tx.readcommited.ReadCommitedTransactionalEntityFile;

public class ReadCommitedEntityFileTransaction 
	extends AbstractEntityFileTransaction{

	public ReadCommitedEntityFileTransaction(
			EntityFileTransactionManager entityFileTransactionManager,
			LockProvider lockProvider,
			Map<EntityFileAccess<?, ?>, TransactionalEntityFile<?, ?>> transactionFiles,
			byte status, long transactionID, boolean started,
			boolean rolledBack, boolean commited, long timeout) {
		super(entityFileTransactionManager, lockProvider, transactionFiles, status,
				transactionID, EntityFileTransaction.TRANSACTION_READ_COMMITED, 
				started, rolledBack, commited, timeout);
	}
	
	protected <T,R> TransactionalEntityFile<T,R> createTransactionalEntityFile(
			EntityFileAccess<T,R> entityFile, TransactionEntityFileAccess<T,R> txFile){
		return 
			new ReadCommitedTransactionalEntityFile<T, R>(
					txFile, this.lockProvider, this.timeout);
	}
	
}
