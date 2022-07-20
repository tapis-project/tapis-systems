/*
 * This file is generated by jOOQ.
 */
package edu.utexas.tacc.tapis.systems.gen.jooq.tables;


import edu.utexas.tacc.tapis.systems.gen.jooq.Keys;
import edu.utexas.tacc.tapis.systems.gen.jooq.TapisSys;
import edu.utexas.tacc.tapis.systems.gen.jooq.tables.records.SystemsLoginUserRecord;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import org.jooq.Field;
import org.jooq.ForeignKey;
import org.jooq.Name;
import org.jooq.Record;
import org.jooq.Row7;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.TableOptions;
import org.jooq.UniqueKey;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.TableImpl;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class SystemsLoginUser extends TableImpl<SystemsLoginUserRecord> {

    private static final long serialVersionUID = 1L;

    /**
     * The reference instance of <code>tapis_sys.systems_login_user</code>
     */
    public static final SystemsLoginUser SYSTEMS_LOGIN_USER = new SystemsLoginUser();

    /**
     * The class holding records for this type
     */
    @Override
    public Class<SystemsLoginUserRecord> getRecordType() {
        return SystemsLoginUserRecord.class;
    }

    /**
     * The column <code>tapis_sys.systems_login_user.system_seq_id</code>.
     */
    public final TableField<SystemsLoginUserRecord, Integer> SYSTEM_SEQ_ID = createField(DSL.name("system_seq_id"), SQLDataType.INTEGER, this, "");

    /**
     * The column <code>tapis_sys.systems_login_user.tenant</code>.
     */
    public final TableField<SystemsLoginUserRecord, String> TENANT = createField(DSL.name("tenant"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>tapis_sys.systems_login_user.system_id</code>.
     */
    public final TableField<SystemsLoginUserRecord, String> SYSTEM_ID = createField(DSL.name("system_id"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>tapis_sys.systems_login_user.tapis_user</code>.
     */
    public final TableField<SystemsLoginUserRecord, String> TAPIS_USER = createField(DSL.name("tapis_user"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>tapis_sys.systems_login_user.login_user</code>.
     */
    public final TableField<SystemsLoginUserRecord, String> LOGIN_USER = createField(DSL.name("login_user"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>tapis_sys.systems_login_user.created</code>.
     */
    public final TableField<SystemsLoginUserRecord, LocalDateTime> CREATED = createField(DSL.name("created"), SQLDataType.LOCALDATETIME(6).nullable(false).defaultValue(DSL.field("timezone('utc'::text, now())", SQLDataType.LOCALDATETIME)), this, "");

    /**
     * The column <code>tapis_sys.systems_login_user.updated</code>.
     */
    public final TableField<SystemsLoginUserRecord, LocalDateTime> UPDATED = createField(DSL.name("updated"), SQLDataType.LOCALDATETIME(6).nullable(false).defaultValue(DSL.field("timezone('utc'::text, now())", SQLDataType.LOCALDATETIME)), this, "");

    private SystemsLoginUser(Name alias, Table<SystemsLoginUserRecord> aliased) {
        this(alias, aliased, null);
    }

    private SystemsLoginUser(Name alias, Table<SystemsLoginUserRecord> aliased, Field<?>[] parameters) {
        super(alias, null, aliased, parameters, DSL.comment(""), TableOptions.table());
    }

    /**
     * Create an aliased <code>tapis_sys.systems_login_user</code> table reference
     */
    public SystemsLoginUser(String alias) {
        this(DSL.name(alias), SYSTEMS_LOGIN_USER);
    }

    /**
     * Create an aliased <code>tapis_sys.systems_login_user</code> table reference
     */
    public SystemsLoginUser(Name alias) {
        this(alias, SYSTEMS_LOGIN_USER);
    }

    /**
     * Create a <code>tapis_sys.systems_login_user</code> table reference
     */
    public SystemsLoginUser() {
        this(DSL.name("systems_login_user"), null);
    }

    public <O extends Record> SystemsLoginUser(Table<O> child, ForeignKey<O, SystemsLoginUserRecord> key) {
        super(child, key, SYSTEMS_LOGIN_USER);
    }

    @Override
    public Schema getSchema() {
        return TapisSys.TAPIS_SYS;
    }

    @Override
    public UniqueKey<SystemsLoginUserRecord> getPrimaryKey() {
        return Keys.SYSTEMS_LOGIN_USER_PKEY;
    }

    @Override
    public List<UniqueKey<SystemsLoginUserRecord>> getKeys() {
        return Arrays.<UniqueKey<SystemsLoginUserRecord>>asList(Keys.SYSTEMS_LOGIN_USER_PKEY);
    }

    @Override
    public List<ForeignKey<SystemsLoginUserRecord, ?>> getReferences() {
        return Arrays.<ForeignKey<SystemsLoginUserRecord, ?>>asList(Keys.SYSTEMS_LOGIN_USER__SYSTEMS_LOGIN_USER_SYSTEM_SEQ_ID_FKEY);
    }

    private transient Systems _systems;

    public Systems systems() {
        if (_systems == null)
            _systems = new Systems(this, Keys.SYSTEMS_LOGIN_USER__SYSTEMS_LOGIN_USER_SYSTEM_SEQ_ID_FKEY);

        return _systems;
    }

    @Override
    public SystemsLoginUser as(String alias) {
        return new SystemsLoginUser(DSL.name(alias), this);
    }

    @Override
    public SystemsLoginUser as(Name alias) {
        return new SystemsLoginUser(alias, this);
    }

    /**
     * Rename this table
     */
    @Override
    public SystemsLoginUser rename(String name) {
        return new SystemsLoginUser(DSL.name(name), null);
    }

    /**
     * Rename this table
     */
    @Override
    public SystemsLoginUser rename(Name name) {
        return new SystemsLoginUser(name, null);
    }

    // -------------------------------------------------------------------------
    // Row7 type methods
    // -------------------------------------------------------------------------

    @Override
    public Row7<Integer, String, String, String, String, LocalDateTime, LocalDateTime> fieldsRow() {
        return (Row7) super.fieldsRow();
    }
}