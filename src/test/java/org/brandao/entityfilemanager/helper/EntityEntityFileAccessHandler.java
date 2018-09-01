package org.brandao.entityfilemanager.helper;

import java.io.IOException;

import org.brandao.entityfilemanager.DataReader;
import org.brandao.entityfilemanager.DataWritter;
import org.brandao.entityfilemanager.EntityFileDataHandler;

public class EntityEntityFileAccessHandler 
	implements EntityFileDataHandler<Entity, byte[], EntityEntityFileAccessHeader>{

	private static final int ROW_LENGTH = 151;

	private static final int DATA_LENGTH = 150;
	
	private static final int FIRST_POINTER = 0;
	
	private static final int HEADER_LENGTH = 0;
	
	private static final int EOF_LENGTH = 1;
	
	private static final int FIRST_RECORD = HEADER_LENGTH;
	
	private byte[] emptyEntity;
	
	private byte[] readBuffer;
	
	public EntityEntityFileAccessHandler(){
		emptyEntity = new byte[DATA_LENGTH];
		readBuffer  = new byte[DATA_LENGTH];
	}
	
	public void writeMetaData(DataWritter stream,
			EntityEntityFileAccessHeader value) throws IOException {
	}

	public EntityEntityFileAccessHeader readMetaData(DataReader srteam)
			throws IOException {
		return new EntityEntityFileAccessHeader();
	}

	public void writeEOF(DataWritter stream) throws IOException {
		stream.writeByte((byte)-1);
	}

	public void write(DataWritter stream, Entity entity) throws IOException {
		stream.writeByte((byte)(entity == null? 0 : 1));
		if(entity == null){
			stream.write(emptyEntity);
		}
		else{
			stream.writeInt(entity.getId());
			stream.writeInt(entity.getStatus());
			stream.writeString(entity.getMessage(), 142);
		}
	}

	public void writeRaw(DataWritter stream, byte[] entity) throws IOException {
		stream.write(entity);
	}

	public Entity read(DataReader stream) throws IOException {
		boolean isnull = stream.readByte() == 0;
		
		if(isnull){
			stream.read(readBuffer);
			return null;
		}
		else{
			Entity e = new Entity();
			e.setId(stream.readInt());
			e.setStatus(stream.readInt());
			e.setMessage(stream.readString(142));
			return e;
		}
		
	}

	public byte[] readRaw(DataReader stream) throws IOException {
		byte[] buffer = new byte[ROW_LENGTH];
		stream.read(buffer);
		return buffer;
	}

	public long getFirstPointer() {
		return FIRST_POINTER;
	}

	public int getHeaderLength() {
		return HEADER_LENGTH;
	}

	public int getRecordLength() {
		return ROW_LENGTH;
	}

	public int getEOFLength() {
		return EOF_LENGTH;
	}

	public int getFirstRecord() {
		return FIRST_RECORD;
	}

	public Class<Entity> getType() {
		return Entity.class;
	}

	public Class<byte[]> getRawType() {
		return byte[].class;
	}

}
