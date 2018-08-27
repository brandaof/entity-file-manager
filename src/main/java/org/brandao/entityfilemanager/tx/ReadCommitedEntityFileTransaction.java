package org.brandao.entityfilemanager.tx;

import java.util.Map;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.LockProvider;

public class ReadCommitedEntityFileTransaction 
	extends AbstractEntityFileTransaction{

	private static final long serialVersionUID = 2522451551205283541L;

	public ReadCommitedEntityFileTransaction(
			EntityFileTransactionManagerConfigurer entityFileTransactionManager,
			LockProvider lockProvider,
			Map<EntityFileAccess<?, ?, ?>, TransactionEntity<?, ?>> transactionFiles,
			byte status, long transactionID, boolean started,
			boolean rolledBack, boolean commited, long timeout, boolean recoveredTransaction) {
		super(entityFileTransactionManager, lockProvider, transactionFiles, status,
				transactionID, EntityFileTransaction.TRANSACTION_READ_COMMITED, 
				started, rolledBack, commited, timeout, recoveredTransaction);
	}

}
