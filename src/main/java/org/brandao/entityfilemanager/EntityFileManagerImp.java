package org.brandao.entityfilemanager;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.brandao.entityfilemanager.tx.EntityFileTransaction;
import org.brandao.entityfilemanager.tx.EntityFileTransactionManager;
import org.brandao.entityfilemanager.tx.TransactionException;

public class EntityFileManagerImp 
	implements EntityFileManagerConfigurer{

	private File path;
	
	private Map<String, EntityFileAccess<?,?>> entities;

	private EntityFileTransactionManager transactioManager;
	
	private boolean started;
	
	private LockProvider lockProvider;
	
	public EntityFileManagerImp(){
		this.entities     = new HashMap<String, EntityFileAccess<?,?>>();
		this.started      = false;
	}
	
	public void setPath(File value) {
		this.path = value;
		
		if(!this.path.exists()){
			this.path.mkdirs();
		}

	}

	public void setLockProvider(LockProvider provider) {
		this.lockProvider = provider;
	}

	public LockProvider getLockProvider() {
		return this.lockProvider;
	}
	
	public void init() throws EntityFileManagerException{
		
		if(this.started)
			throw new EntityFileManagerException("manager has been started");
		
		try{
			this.transactioManager.init();
			this.started = true;
		}
		catch(EntityFileManagerException ex){
			this.started = false;
			throw ex;
		}
		catch(Throwable ex){
			this.started = false;
			throw new EntityFileManagerException(ex);
		}
	}

	public void destroy() throws EntityFileManagerException{
		
		if(!this.started)
			throw new EntityFileManagerException("manager not started");

		try{
			this.transactioManager.destroy();
			
			for(EntityFileAccess<?,?> entityFile: this.entities.values()){
				entityFile.close();
			}
			
			this.started = false;
		}
		catch(Throwable e){
			throw new EntityFileManagerException(e);
		}
		
	}
	
	public void create(String name, EntityFileAccess<?,?> entityFile) throws EntityFileManagerException{
		
		try{
			if(entityFile.exists())
				entityFile.open();
			else
				entityFile.createNewFile();
			
			this.entities.put(name, entityFile);
		}
		catch(Throwable e){
			throw new EntityFileManagerException(e);
		}
	}

	public EntityFileAccess<?, ?> getEntityFile(String name)
			throws EntityFileManagerException {
		return this.entities.get(name);
	}
	
	public void remove(String name) throws EntityFileManagerException{
		
		EntityFileAccess<?,?> entity = this.entities.get(name);
		
		try{
			entity.close();
		}
		catch(Throwable e){
			throw new EntityFileManagerException(e);
		}
	}

	@SuppressWarnings("unchecked")
	public <T> EntityFile<T> getEntityFile(String name, EntityFileTransaction tx, Class<T> type){
		
		if(!this.started)
			throw new EntityFileManagerException("manager not started");
		
		EntityFileAccess<T,?> fileAccess = (EntityFileAccess<T,?>) this.entities.get(name); 
		return new EntityFileTX<T>(fileAccess, tx);
	}

	public File getPath() {
		return this.path;
	}

	public void setEntityFileTransactionManager(EntityFileTransactionManager transactioMmanager){
		this.transactioManager = transactioMmanager;
	}

	public EntityFileTransactionManager getEntityFileTransactionManager(){
		return this.transactioManager;
	}
	
	public void truncate(String name) throws EntityFileManagerException {
		try{
			EntityFileAccess<?,?> entity = this.entities.get(name);
			entity.createNewFile();
		}
		catch(Throwable e){
			throw new EntityFileManagerException(e);
		}
	}

	public EntityFileTransaction beginTransaction() throws TransactionException{
		return this.transactioManager.openTransaction();
	}

	public static class EntityFileTX<T> implements EntityFile<T>{

		private EntityFileTransaction tx;
		
		private EntityFileAccess<T,?> entityFile;
		
		public EntityFileTX(EntityFileAccess<T,?> entityFile, EntityFileTransaction tx) {
			this.tx = tx;
			this.entityFile = entityFile;
		}

		public long insert(T entity) throws IOException {
			return this.tx.insert(entity, entityFile);
		}

		public void update(long id, T entity) throws IOException {
			this.tx.update(id, entity, entityFile);
		}

		public void delete(long id) throws IOException {
			this.tx.delete(id, entityFile);
		}

		public T select(long id) throws IOException {
			return this.tx.select(id, entityFile);		
		}
		
	}

}
