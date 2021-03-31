package com.infomaximum.database.utils;

import com.infomaximum.database.domainobject.DomainObjectSource;
import com.infomaximum.rocksdb.RocksDBProvider;

@FunctionalInterface
public interface DomainBiConsumer {

    void accept(DomainObjectSource domainObjectSource, RocksDBProvider rocksDBProvider) throws Exception;
}