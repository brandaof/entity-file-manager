package org.brandao.entityfilemanager.tx;

import java.util.Map;

import org.brandao.entityfilemanager.EntityFileAccess;

public interface EntityFileTransactionManager {

	EntityFileTransaction create();

	EntityFileTransaction load(
			Map<EntityFileAccess<?, ?>, TransactionalEntityFile<?, ?>> transactionFiles,
			byte status, long transactionID, boolean started, boolean rolledBack, boolean commited);
	
	void close(EntityFileTransaction tx);
	
}
