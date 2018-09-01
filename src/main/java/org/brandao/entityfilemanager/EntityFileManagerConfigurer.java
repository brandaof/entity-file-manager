package org.brandao.entityfilemanager;

import java.io.File;

import org.brandao.entityfilemanager.tx.EntityFileTransactionManager;

public interface EntityFileManagerConfigurer 
	extends EntityFileManager{

	void setEntityFileTransactionManager(EntityFileTransactionManager factory);

	EntityFileTransactionManager getEntityFileTransactionManager();
	
	void setLockProvider(LockProvider provider);
	
	LockProvider getLockProvider();
	
	void setPath(File value);

	File getPath();

	void register(EntityFileAccess<?,?,?> entityFile) throws EntityFileManagerException;

	void unregister(String name) throws EntityFileManagerException;
	
	void truncate(String name) throws EntityFileManagerException;
	
	EntityFileAccess<?,?,?> getEntityFile(String name) throws EntityFileManagerException;
	
	void init() throws EntityFileManagerException;
	
	void destroy() throws EntityFileManagerException;
	
}
