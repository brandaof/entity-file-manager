# entity-file-manager
Persists entities in structured files with transactional support.

Example:

```
File path   = new File("./data");
File txPath = new File(path, "tx");

EntityFileManagerConfigurer efm = new EntityFileManagerImp();

LockProvider lp = new LockProviderImp();

EntityFileTransactionManagerConfigurer tm = new EntityFileTransactionManagerImp();

tm.setLockProvider(lp);
tm.setTimeout(EntityFileTransactionManagerImp.DEFAULY_TIME_OUT);
tm.setTransactionPath(txPath);
tm.setEntityFileManagerConfigurer(efm);

efm.setEntityFileTransactionManager(tm);
efm.setLockProvider(lp);
efm.setPath(path);
efm.register("long", new LongEntityFileAccess(new File(path, "long")));
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
