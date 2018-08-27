package org.brandao.entityfilemanager;

import java.io.File;

public class SimpleEntityFileAccess<T, R, H> 
	extends AbstractEntityFileAccess<T, R, H>{

	public SimpleEntityFileAccess(String name, File file,
			EntityFileDataHandler<T, R, H> dataHandler) {
		super(name, file, dataHandler);
	}

}
