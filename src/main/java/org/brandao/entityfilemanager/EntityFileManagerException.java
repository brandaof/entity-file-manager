package org.brandao.entityfilemanager;

public class EntityFileManagerException 
	extends RuntimeException{

	private static final long serialVersionUID = -8911891466583065905L;

	public EntityFileManagerException() {
		super();
	}

	public EntityFileManagerException(String message, Throwable cause) {
		super(message, cause);
	}

	public EntityFileManagerException(String message) {
		super(message);
	}

	public EntityFileManagerException(Throwable cause) {
		super(cause);
	}


}
