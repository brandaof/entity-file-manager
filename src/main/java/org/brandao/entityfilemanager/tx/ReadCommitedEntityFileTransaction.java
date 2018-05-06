package org.brandao.entityfilemanager.tx;

import java.util.Map;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.EntityFileException;
import org.brandao.entityfilemanager.LockProvider;
import org.brandao.entityfilemanager.tx.readcommited.TransactionalEntityFile;

public class ReadCommitedEntityFileTransaction 
	extends AbstractEntityFileTransaction{

	public ReadCommitedEntityFileTransaction(
			EntityFileTransactionManager entityFileTransactionManager,
			LockProvider lockProvider,
			Map<EntityFileAccess<?, ?>, TransactionalEntityFile<?, ?>> transactionFiles,
			byte status, long transactionID, boolean started,
			boolean rolledBack, boolean commited, long timeout) {
		super(entityFileTransactionManager, lockProvider, transactionFiles, status,
				transactionID, started, rolledBack, commited, timeout);
	}

	public <T, R> long insert(T entity, EntityFileAccess<T, R> entityFileAccess)
			throws EntityFileException {
		TransactionalEntityFile<T,R> tei = this.getManagedEntityFile(entityFileAccess);
		return tei.insert(entity);
	}

	public <T, R> long[] insert(T[] entity,
			EntityFileAccess<T, R> entityFileAccess) throws EntityFileException {
		TransactionalEntityFile<T,R> tei = this.getManagedEntityFile(entityFileAccess);
		return tei.insert(entity);
	}

	public <T, R> void update(long id, T entity,
			EntityFileAccess<T, R> entityFileAccess) throws EntityFileException {
		TransactionalEntityFile<T,R> tei = this.getManagedEntityFile(entityFileAccess);
		tei.update(id, entity);
	}

	public <T, R> void update(long[] id, T[] entity,
			EntityFileAccess<T, R> entityFileAccess) throws EntityFileException {
		TransactionalEntityFile<T,R> tei = this.getManagedEntityFile(entityFileAccess);
		tei.update(id, entity);
	}

	public <T, R> void delete(long id, EntityFileAccess<T, R> entityFileAccess)
			throws EntityFileException {
		TransactionalEntityFile<T,R> tei = this.getManagedEntityFile(entityFileAccess);
		tei.delete(id);
	}

	public <T, R> void delete(long[] id, EntityFileAccess<T, R> entityFileAccess)
			throws EntityFileException {
		TransactionalEntityFile<T,R> tei = this.getManagedEntityFile(entityFileAccess);
		tei.delete(id);
	}

	public <T, R> T select(long id, EntityFileAccess<T, R> entityFileAccess)
			throws EntityFileException {
		TransactionalEntityFile<T,R> tei = this.getManagedEntityFile(entityFileAccess);
		return tei.select(id);
	}

	public <T, R> T[] select(long[] id, EntityFileAccess<T, R> entityFileAccess)
			throws EntityFileException {
		TransactionalEntityFile<T,R> tei = this.getManagedEntityFile(entityFileAccess);
		return tei.select(id);
	}

	public <T, R> T select(long id, boolean lock,
			EntityFileAccess<T, R> entityFileAccess) throws EntityFileException {
		TransactionalEntityFile<T,R> tei = this.getManagedEntityFile(entityFileAccess);
		return tei.select(id, lock);
	}

	public <T, R> T[] select(long[] id, boolean lock,
			EntityFileAccess<T, R> entityFileAccess) throws EntityFileException {
		TransactionalEntityFile<T,R> tei = this.getManagedEntityFile(entityFileAccess);
		return tei.select(id, lock);
	}
	
}
