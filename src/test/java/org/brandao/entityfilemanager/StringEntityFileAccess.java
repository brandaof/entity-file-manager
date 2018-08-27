package org.brandao.entityfilemanager;

import java.io.File;
import java.io.IOException;

import org.brandao.entityfilemanager.StringEntityFileAccess.*;

public class StringEntityFileAccess 
	extends SimpleEntityFileAccess<String, byte[], StringEntityFileAccessHeader>{

	public StringEntityFileAccess(String name, File file) {
		super(name, file, new StringEntityFileAccessHandler());
		this.metadata = new StringEntityFileAccessHeader();
	}

	public static class StringEntityFileAccessHandler 
		implements EntityFileDataHandler<String, byte[], StringEntityFileAccessHeader>{

		public void writeMetaData(DataOutputStream stream,
				StringEntityFileAccessHeader value) throws IOException {
		}
	
		public StringEntityFileAccessHeader readMetaData(DataInputStream srteam)
				throws IOException {
			return new StringEntityFileAccessHeader();
		}
	
		public void writeEOF(DataOutputStream stream) throws IOException {
			stream.writeByte((byte)-1);
		}
	
		public void write(DataOutputStream stream, String entity) throws IOException {
			stream.writeString(entity, 256);
		}
	
		public void writeRaw(DataOutputStream stream, byte[] entity)
				throws IOException {
			stream.write(entity);
		}
	
		public String read(DataInputStream stream) throws IOException {
			return stream.readString(256).trim();
		}
	
		public byte[] readRaw(DataInputStream stream) throws IOException {
			byte[] b = new byte[256];
			stream.read(b);
			return b;
		}
	
		public int getRecordLength() {
			return 256;
		}
	
		public int getEOFLength() {
			return 1;
		}
	
		public int getFirstRecord() {
			return 0;
		}
	
		public Class<String> getType() {
			return String.class;
		}
	
		public Class<byte[]> getRawType() {
			return byte[].class;
		}

		public int getHeaderLength() {
			// TODO Auto-generated method stub
			return 0;
		}
	
	}

	public static class StringEntityFileAccessHeader{
	}
	
	
}
