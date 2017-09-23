package com.infomaximum.database.datasource.entitysource;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by kris on 01.05.17.
 */
public interface EntitySource extends Serializable {

    public long getId();

    public Map<String, byte[]> getFields();

}
