# Entity file manager
Permite registrar entidades em arquivos estruturados que possuem suporte transacional.

Como usar:

1.  Criar a implementação do EntityFileDataHandler;
2.  Criar o header do arquivo;
3.  Instanciar alguma implementação do EntityFileAccess;
4.  Iniciar o EntityFileManager e registrar o EntityFileAccess.


Exemplo:

### EntityFileDataHandler

```
public class LongEntityFileAccessHandler 
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
	
}
```
### Header

```
public static class LongEntityFileAccessHeader{
}
```

### Instanciando o EntityFileAccess

```
File longFile = ...;
EntityFileAccess<Long, byte[], LongEntityFileAccessHeader> longEntityFileAccess =
    new SimpleEntityFileAccess<Long, byte[], LongEntityFileAccessHeader>(
        longFile, new LongEntityFileAccessHandler())

``` 

### Iniciando o EntityFileManager

```
File path   = new File("./data");
File txPath = new File(path, "tx");

EntityFileManagerConfigurer efm = new EntityFileManagerImp();

LockProvider lp = new LockProviderImp();

EntityFileTransactionManagerConfigurer tm = new EntityFileTransactionManagerImp();

tm.setLockProvider(lp);
tm.setTimeout(EntityFileTransactionManagerImp.DEFAULT_TIMEOUT);
tm.setTransactionPath(txPath);
tm.setEntityFileManagerConfigurer(efm);

efm.setEntityFileTransactionManager(tm);
efm.setLockProvider(lp);
efm.setPath(path);
efm.register("long", longEntityFileAccess);
efm.init();


EntityFileTransaction tx = efm.beginTransaction();
try{
    EntityFile<Long> ef = efm.getEntityFile("long", tx, Long.class);
    ef.insert(0L);
    ef.insert(198563254512664L);
    ef.insert(152326598598562L);
}
finally{
    tx.commit();
}
```    
