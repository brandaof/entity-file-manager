package org.brandao.entityfilemanager;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.brandao.entityfilemanager.tx.EntityFileTransaction;
import org.brandao.entityfilemanager.tx.EntityFileTransactionManager;
import org.brandao.entityfilemanager.tx.TransactionException;

public class EntityFileManagerImp 
	implements EntityFileManagerConfigurer{

	private File path;
	
	private Map<String, EntityFileAccess<?,?,?>> entities;

	private EntityFileTransactionManager transactioManager;
	
	private boolean started;
	
	private LockProvider lockProvider;
	
	public EntityFileManagerImp(){
		this.entities     = new HashMap<String, EntityFileAccess<?,?,?>>();
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
			
			for(EntityFileAccess<?,?,?> entityFile: this.entities.values()){
				entityFile.close();
			}
			
			this.started = false;
		}
		catch(Throwable e){
			throw new EntityFileManagerException(e);
		}
		
	}
	
	public void register(EntityFileAccess<?,?,?> entityFile) throws EntityFileManagerException{
		
		try{
			if(entityFile.exists())
				entityFile.open();
			else
				entityFile.createNewFile();
			
			this.entities.put(entityFile.getName(), entityFile);
		}
		catch(Throwable e){
			throw new EntityFileManagerException(e);
		}
	}

	public void unregister(String name) throws EntityFileManagerException{
		
		try{
			EntityFileAccess<?,?,?> entity = this.entities.get(name);
			
			if(entity != null){
				entity.close();
			}
		}
		catch(Throwable e){
			throw new EntityFileManagerException(e);
		}
	}
	
	public EntityFileAccess<?, ?,?> getEntityFile(String name)
			throws EntityFileManagerException {
		return this.entities.get(name);
	}
	
	@SuppressWarnings("unchecked")
	public <T> EntityFile<T> getEntityFile(String name, EntityFileTransaction tx, Class<T> type){
		
		EntityFileAccess<T,?,?> fileAccess = (EntityFileAccess<T,?,?>) this.entities.get(name);
		
		if(fileAccess == null){
			throw new EntityFileManagerException("not found: " + name);
		}
		
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
			EntityFileAccess<?,?,?> entity = this.entities.get(name);
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
		
		private EntityFileAccess<T,?,?> entityFileAccess;
		
		public EntityFileTX(EntityFileAccess<T,?,?> entityFileAccess, EntityFileTransaction tx) {
			this.tx = tx;
			this.entityFileAccess = entityFileAccess;
		}

		public long insert(T entity) throws EntityFileException {
			return tx.insert(entity, entityFileAccess);
		}

		public long[] insert(T[] entity) throws EntityFileException {
			return tx.insert(entity, entityFileAccess);
		}

		public void update(long id, T entity) throws EntityFileException {
			tx.update(id, entity, entityFileAccess);
		}

		public void update(long[] id, T[] entity) throws EntityFileException {
			tx.update(id, entity, entityFileAccess);
		}

		public void delete(long id) throws EntityFileException {
			tx.delete(id, entityFileAccess);
		}

		public void delete(long[] id) throws EntityFileException {
			tx.delete(id, entityFileAccess);
		}

		public T select(long id) throws EntityFileException {
			return tx.select(id, entityFileAccess);
		}

		public T[] select(long[] id) throws EntityFileException {
			return tx.select(id, entityFileAccess);
		}

		public T select(long id, boolean lock) throws EntityFileException {
			return tx.select(id, lock, entityFileAccess);
		}

		public T[] select(long[] id, boolean lock) throws EntityFileException {
			return tx.select(id, lock, entityFileAccess);
		}
		
	}

}
