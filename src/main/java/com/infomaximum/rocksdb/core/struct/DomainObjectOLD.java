package com.infomaximum.rocksdb.core.struct;

import com.infomaximum.database.core.transaction.Transaction;
import com.infomaximum.rocksdb.core.datasource.DataSource;
import com.infomaximum.rocksdb.utils.ProxyDomainObjectUtils;

import java.lang.reflect.Field;
import java.util.Set;

/**
 * Created by kris on 19.04.17.
 */
public abstract class DomainObjectOLD {

    private final long id;

    private DataSource dataSource = null;
    private Transaction transaction = null;

    /**
     * Внутренне состояние - содержит поля которые не были загружены
     */
    private Set<Field> lazyLoads=null;

    /**
     * Внутренне состояние - содержит поля которые были отредактированы в рамках транзакции
     */
    private Set<Field> updatesField=null;


    public DomainObjectOLD(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public boolean isReadOnly() {
        return (transaction==null || !transaction.isActive());
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append(getClass().getSuperclass().getName()).append('(')
                .append("id: ").append(id)
                .append(')').toString();
    }

    public void save(){}

    public void remove(){}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!DomainObjectOLD.class.isAssignableFrom(o.getClass())) return false;
        if (o == null || ProxyDomainObjectUtils.getProxySuperClassOLD(getClass()) != ProxyDomainObjectUtils.getProxySuperClassOLD((Class<? extends DomainObjectOLD>) o.getClass())) return false;
        DomainObjectOLD that = (DomainObjectOLD) o;
        return id == that.id;
    }

    @Override
    public int hashCode() {
        return (int) (id ^ (id >>> 32));
    }
}

