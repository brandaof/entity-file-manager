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

		public void writeMetaData(DataOutputStream stream,
				LongEntityFileAccessHeader value) throws IOException {
		}
	
		public LongEntityFileAccessHeader readMetaData(DataInputStream srteam)
				throws IOException {
			return new LongEntityFileAccessHeader();
		}
	
		public void writeEOF(DataOutputStream stream) throws IOException {
			stream.writeByte((byte)-1);
		}
	
		public void write(DataOutputStream stream, Long entity) throws IOException {
			stream.writeLong(entity == null? 0L : entity);
		}
	
		public void writeRaw(DataOutputStream stream, byte[] entity)
				throws IOException {
			stream.write(entity);
		}
	
		public Long read(DataInputStream stream) throws IOException {
			return stream.readLong();
		}
	
		public byte[] readRaw(DataInputStream stream) throws IOException {
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
