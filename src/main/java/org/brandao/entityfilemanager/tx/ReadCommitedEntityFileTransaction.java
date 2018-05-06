package org.brandao.entityfilemanager.tx;

import java.util.Map;

import org.brandao.entityfilemanager.EntityFileAccess;
import org.brandao.entityfilemanager.EntityFileException;
import org.brandao.entityfilemanager.LockProvider;

public class ReadCommitedEntityFileTransaction 
	extends AbstractEntityFileTransaction{

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
		return 0;
	}

	public <T, R> long[] insert(T[] entity,
			EntityFileAccess<T, R> entityFileAccess) throws EntityFileException {
		// TODO Auto-generated method stub
		return null;
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

	/*
	public <T,R> long insert(T entity, EntityFileAccess<T,R> entityFileaccess)
			 throws EntityFileException {
		
		TransactionalEntityFileInfo<T,R> tei    = this.getManagedEntityFile(entityFileaccess);
		TransactionalEntityFile<T,R> entityFile = tei.getEntityFile();
		PointerManager<T, R> pointerManager     = tei.getPointerManager();

		this.lockProvider.lock(entityFileaccess);
		try{
			long pointer = entityFile.insert(entity);
			pointerManager.managerPointer(pointer, true);
			return pointer;
		}
		catch(EntityFileException e){
			this.dirty = true;
			throw e;
		}
		catch(Throwable e){
			this.dirty = true;
			throw new EntityFileException(e);
		}
		finally{
			this.lockProvider.unlock(entityFileaccess);
		}
		
	}

	public <T,R> void update(long id, T entity,
			EntityFileAccess<T,R> entityFileaccess) throws EntityFileException {
		
		TransactionalEntityFileInfo<T,R> tei    = this.getManagedEntityFile(entityFileaccess);
		TransactionalEntityFile<T,R> entityFile = tei.getEntityFile();
		PointerManager<T, R> pointerManager     = tei.getPointerManager();
		
		this.lockProvider.tryLock(entityFileaccess, id, this.timeout, TimeUnit.MILLISECONDS);
		
		try{
			pointerManager.managerPointer(id, false);
			txEntityFileAccess.update(id, entity);
		}
		catch(PersistenceException e){
			this.dirty = true;
			throw e;
		}
		catch(Throwable e){
			this.dirty = true;
			throw new PersistenceException(e);
		}
	}

	public <T,R> void delete(long id, EntityFileAccess<T,R> entityFileaccess)
			throws PersistenceException {
		try{
			this.lockProvider.tryLock(entityFileaccess, id, this.timeout, TimeUnit.MILLISECONDS);
			TransactionalEntityFile<T,R> txEntityFileAccess = this.getManagedEntityFile(entityFileaccess);
			txEntityFileAccess.delete(id);
		}
		catch(PersistenceException e){
			this.dirty = true;
			throw e;
		}
		catch(Throwable e){
			this.dirty = true;
			throw new PersistenceException(e);
		}
	}

	public <T,R> T select(long id, EntityFileAccess<T,R> entityFileaccess) {
		TransactionalEntityFile<T,R> txEntityFileAccess = this.getManagedEntityFile(entityFileaccess);
		return txEntityFileAccess.select(id);
	}

	public <T,R> T select(long id, boolean lock, EntityFileAccess<T,R> entityFileaccess) {
		try{
			this.lockProvider.tryLock(entityFileaccess, id, this.timeout, TimeUnit.MILLISECONDS);
			TransactionalEntityFile<T,R> txEntityFileAccess = this.getManagedEntityFile(entityFileaccess);
			return txEntityFileAccess.select(id);
		}
		catch(PersistenceException e){
			this.dirty = true;
			throw e;
		}
		catch(Throwable e){
			this.dirty = true;
			throw new PersistenceException(e);
		}
	}
	
	public void close() throws TransactionException{
		entityFileTransactionManager.closeTransaction(this);
	}
	
*/
	
}
