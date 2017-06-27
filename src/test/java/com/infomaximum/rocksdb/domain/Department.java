package com.infomaximum.rocksdb.domain;

import com.infomaximum.rocksdb.core.anotation.Entity;
import com.infomaximum.rocksdb.core.anotation.EntityField;
import com.infomaximum.rocksdb.core.anotation.Index;
import com.infomaximum.rocksdb.core.struct.DomainObject;

/**
 * Created by kris on 26.04.17.
 */
@Entity(
        columnFamily = "com.infomaximum.subsystem.employee.Department",
        indexes = {
                @Index(fieldNames = {"parent"})
        }
)
public class Department extends DomainObject {

    @EntityField
    private String name;

    @EntityField(lazy = true)
    private Department parent;

    public Department(long id) {
        super(id);
    }


    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }


    public Department getParent() {
        return parent;
    }
    public void setParent(Department parent) {
        this.parent = parent;
    }

}
