package org.brandao.entityfilemanager;

import java.io.File;
import java.io.IOException;

import org.brandao.entityfilemanager.LongEntityFileAccess.*;

public class LongEntityFileAccess 
	extends SimpleEntityFileAccess<Long, byte[], LongEntityFileAccessHeader>{

	public LongEntityFileAccess(String name, File file) {
		super(name, file, new LongEntityFileAccessHandler());
		this.metadata = new LongEntityFileAccessHeader();
	}

	public static class LongEntityFileAccessHandler 
		implements EntityFileDataHandler<Long, byte[], LongEntityFileAccessHeader>{

		public void writeMetaData(DataWritter stream,
				LongEntityFileAccessHeader value) throws IOException {
		}
	
		public LongEntityFileAccessHeader readMetaData(DataReader srteam)
				throws IOException {
			return new LongEntityFileAccessHeader();
		}
	
		public void writeEOF(DataWritter stream) throws IOException {
			stream.writeByte((byte)-1);
		}
	
		public void write(DataWritter stream, Long entity) throws IOException {
			stream.writeLong(entity == null? 0L : entity);
		}
	
		public void writeRaw(DataWritter stream, byte[] entity)
				throws IOException {
			stream.write(entity);
		}
	
		public Long read(DataReader stream) throws IOException {
			return stream.readLong();
		}
	
		public byte[] readRaw(DataReader stream) throws IOException {
			byte[] b = new byte[8];
			stream.read(b);
			return b;
		}
	
		public int getRecordLength() {
			return 8;
		}
	
		public int getEOFLength() {
			return 1;
		}
	
		public int getFirstRecord() {
			return 0;
		}
	
		public Class<Long> getType() {
			return Long.class;
		}
	
		public Class<byte[]> getRawType() {
			return byte[].class;
		}

		public int getHeaderLength() {
			return (int) this.getFirstRecord();
		}

		public long getFirstPointer() {
			return 0;
		}
	
	}

	public static class LongEntityFileAccessHeader{
	}
	
	
}
