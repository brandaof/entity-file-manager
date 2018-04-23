package org.brandao.entityfilemanager.tx;

import java.io.IOException;

import org.brandao.entityfilemanager.EntityFile;
import org.brandao.entityfilemanager.EntityFileAccess;

public class EntityFileTransactionWrapper<T>
	implements EntityFile<T>{

	private EntityFileAccess<T, ?> entityFileAccess;
	
	private EntityFileTransactionManager entityFileTransactionManager;
	
	public EntityFileTransactionWrapper(
			EntityFileAccess<T, ?> entityFileAccess,
			EntityFileTransactionManager entityFileTransactionManager){
		this.entityFileAccess = entityFileAccess;
		this.entityFileTransactionManager = entityFileTransactionManager;
	}
	
	public long insert(T entity) throws IOException {
		EntityFileTransactionHandler handler = 
				this.entityFileTransactionManager.getCurrent();
		return handler.insert(entity, this.entityFileAccess);
	}

	public void update(long id, T entity) throws IOException {
		EntityFileTransactionHandler handler = 
				this.entityFileTransactionManager.getCurrent();
		handler.update(id, entity, this.entityFileAccess);
	}

	public void delete(long id) throws IOException {
		EntityFileTransactionHandler handler = 
				this.entityFileTransactionManager.getCurrent();
		handler.delete(id, this.entityFileAccess);
	}

	public T select(long id) throws IOException {
		EntityFileTransactionHandler handler = 
				this.entityFileTransactionManager.getCurrent();
		return handler.select(id, this.entityFileAccess);
	}

}
