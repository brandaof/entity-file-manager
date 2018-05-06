package org.brandao.entityfilemanager.tx;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.EntityFileException;
import org.brandao.entityfilemanager.LockProvider;

public class ReadCommitedEntityFileTransaction 
	extends AbstractEntityFileTransaction{

	private Set<Long> pointers;
	
	public ReadCommitedEntityFileTransaction(
			EntityFileTransactionManager entityFileTransactionManager,
			LockProvider lockProvider,
			Map<EntityFileAccess<?, ?>, TransactionalEntityFileInfo<?, ?>> transactionFiles,
			byte status, long transactionID, boolean started,
			boolean rolledBack, boolean commited, long timeout) {
		super(entityFileTransactionManager, lockProvider, transactionFiles, status,
				transactionID, started, rolledBack, commited, timeout);
	}

	public <T, R> long insert(T entity, EntityFileAccess<T, R> entityFileAccess)
			throws EntityFileException {
		
		TransactionalEntityFileInfo<T,R> tei = this.getManagedEntityFile(entityFileAccess);
		
		this.lockProvider.lock(entityFileAccess);
		try{
			TransactionalEntityFile<T, R> tef = tei.getEntityFile();
			PointerManager<T, R> pm = tei.getPointerManager();
			long id = tef.getNextFreePointer(false);
			pm.managerPointer(id, true);
			tef.insert(entity);
			return id;
		}
		catch(Throwable e){
			throw new EntityFileException(e);
		}
		finally{
			this.lockProvider.unlock(entityFileAccess);
		}
		
	}

	public <T, R> long[] insert(T[] entity,
			EntityFileAccess<T, R> entityFileAccess) throws EntityFileException {
		TransactionalEntityFileInfo<T,R> tei = this.getManagedEntityFile(entityFileAccess);
		
		this.lockProvider.lock(entityFileAccess);
		try{
			TransactionalEntityFile<T, R> tef = tei.getEntityFile();
			PointerManager<T, R> pm = tei.getPointerManager();
			long id = tef.getNextFreePointer(true);
			pm.managerPointer(id, true);
			tef.insert(entity);
			return id;
		}
		catch(Throwable e){
			throw new EntityFileException(e);
		}
		finally{
			this.lockProvider.unlock(entityFileAccess);
		}
	}

	public <T, R> void update(long id, T entity,
			EntityFileAccess<T, R> entityFileAccess) throws EntityFileException {
		// TODO Auto-generated method stub
		
	}

	public <T, R> void update(long[] id, T[] entity,
			EntityFileAccess<T, R> entityFileAccess) throws EntityFileException {
		// TODO Auto-generated method stub
		
	}

	public <T, R> void delete(long id, EntityFileAccess<T, R> entityFileAccess)
			throws EntityFileException {
		// TODO Auto-generated method stub
		
	}

	public <T, R> void delete(long[] id, EntityFileAccess<T, R> entityFileAccess)
			throws EntityFileException {
		// TODO Auto-generated method stub
		
	}

	public <T, R> T select(long id, EntityFileAccess<T, R> entityFileAccess)
			throws EntityFileException {
		// TODO Auto-generated method stub
		return null;
	}

	public <T, R> T[] select(long[] id, EntityFileAccess<T, R> entityFileAccess)
			throws EntityFileException {
		// TODO Auto-generated method stub
		return null;
	}

	public <T, R> T select(long id, boolean lock,
			EntityFileAccess<T, R> entityFileAccess) throws EntityFileException {
		// TODO Auto-generated method stub
		return null;
	}

	public <T, R> T[] select(long[] id, boolean lock,
			EntityFileAccess<T, R> entityFileAccess) throws EntityFileException {
		// TODO Auto-generated method stub
		return null;
	}
	
}
