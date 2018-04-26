package org.brandao.entityfilemanager.tx;

public class TransactionalEntityFileInfo<T,R> {

	private TransactionalEntityFile<T,R> entityFile;
	
	private PointerManager<?,?> pointerManager;

	public TransactionalEntityFileInfo(
			TransactionalEntityFile<T, R> entityFile,
			PointerManager<?, ?> pointerManager) {
		this.entityFile = entityFile;
		this.pointerManager = pointerManager;
	}

	public TransactionalEntityFile<T, R> getEntityFile() {
		return entityFile;
	}

	public PointerManager<?, ?> getPointerManager() {
		return pointerManager;
	}
	
	
}
