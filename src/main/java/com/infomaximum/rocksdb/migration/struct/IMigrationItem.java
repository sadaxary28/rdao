package com.infomaximum.rocksdb.migration.struct;

/**
 * Created by kris on 21.07.16.
 */
public interface IMigrationItem {

    public String getMigrationVersion();

    public String getCompletedVersion();

    public void migration() throws Exception;
}
