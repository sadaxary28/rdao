package com.infomaximum.rocksdb.core.datasource.entitysource;

import java.util.Map;

/**
 * Created by kris on 01.05.17.
 */
public interface EntitySource {

    public long getId();

    public Map<String, byte[]> getFields();

}
