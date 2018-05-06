package org.brandao.entityfilemanager.tx;

public class TransactionalEntityFileInfo<T,R> {

	private TransactionalEntityFile<T,R> entityFile;
	
	private PointerManager<T,R> pointerManager;

	public TransactionalEntityFileInfo(
			TransactionalEntityFile<T, R> entityFile,
			PointerManager<T,R> pointerManager) {
		this.entityFile = entityFile;
		this.pointerManager = pointerManager;
	}

	public TransactionalEntityFile<T, R> getEntityFile() {
		return entityFile;
	}

	public PointerManager<T,R> getPointerManager() {
		return pointerManager;
	}
	
	
}
