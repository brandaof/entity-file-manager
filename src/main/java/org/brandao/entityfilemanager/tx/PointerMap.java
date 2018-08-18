package org.brandao.entityfilemanager.tx;

public class PointerMap {

	private long id;
	
	private byte status;

	public PointerMap(long id, byte status) {
		super();
		this.id = id;
		this.status = status;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public byte getStatus() {
		return status;
	}

	public void setStatus(byte status) {
		this.status = status;
	}
	
}
