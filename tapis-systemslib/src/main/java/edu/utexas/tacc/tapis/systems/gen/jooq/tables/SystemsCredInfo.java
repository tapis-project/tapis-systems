/*
 * This file is generated by jOOQ.
 */
package edu.utexas.tacc.tapis.systems.gen.jooq.tables;


import edu.utexas.tacc.tapis.systems.gen.jooq.Keys;
import edu.utexas.tacc.tapis.systems.gen.jooq.TapisSys;
import edu.utexas.tacc.tapis.systems.gen.jooq.tables.records.SystemsCredInfoRecord;
import edu.utexas.tacc.tapis.systems.model.CredentialInfo.SyncStatus;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.jooq.Field;
import org.jooq.ForeignKey;
import org.jooq.Function17;
import org.jooq.Name;
import org.jooq.Record;
import org.jooq.Records;
import org.jooq.Row17;
import org.jooq.Schema;
import org.jooq.SelectField;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.TableOptions;
import org.jooq.UniqueKey;
import org.jooq.impl.DSL;
import org.jooq.impl.EnumConverter;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.TableImpl;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class SystemsCredInfo extends TableImpl<SystemsCredInfoRecord> {

    private static final long serialVersionUID = 1L;

    /**
     * The reference instance of <code>tapis_sys.systems_cred_info</code>
     */
    public static final SystemsCredInfo SYSTEMS_CRED_INFO = new SystemsCredInfo();

    /**
     * The class holding records for this type
     */
    @Override
    public Class<SystemsCredInfoRecord> getRecordType() {
        return SystemsCredInfoRecord.class;
    }

    /**
     * The column <code>tapis_sys.systems_cred_info.system_seq_id</code>.
     */
    public final TableField<SystemsCredInfoRecord, Integer> SYSTEM_SEQ_ID = createField(DSL.name("system_seq_id"), SQLDataType.INTEGER, this, "");

    /**
     * The column <code>tapis_sys.systems_cred_info.tenant</code>.
     */
    public final TableField<SystemsCredInfoRecord, String> TENANT = createField(DSL.name("tenant"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>tapis_sys.systems_cred_info.system_id</code>.
     */
    public final TableField<SystemsCredInfoRecord, String> SYSTEM_ID = createField(DSL.name("system_id"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>tapis_sys.systems_cred_info.tapis_user</code>.
     */
    public final TableField<SystemsCredInfoRecord, String> TAPIS_USER = createField(DSL.name("tapis_user"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>tapis_sys.systems_cred_info.login_user</code>.
     */
    public final TableField<SystemsCredInfoRecord, String> LOGIN_USER = createField(DSL.name("login_user"), SQLDataType.CLOB, this, "");

    /**
     * The column <code>tapis_sys.systems_cred_info.created</code>.
     */
    public final TableField<SystemsCredInfoRecord, LocalDateTime> CREATED = createField(DSL.name("created"), SQLDataType.LOCALDATETIME(6).nullable(false).defaultValue(DSL.field("timezone('utc'::text, now())", SQLDataType.LOCALDATETIME)), this, "");

    /**
     * The column <code>tapis_sys.systems_cred_info.updated</code>.
     */
    public final TableField<SystemsCredInfoRecord, LocalDateTime> UPDATED = createField(DSL.name("updated"), SQLDataType.LOCALDATETIME(6).nullable(false).defaultValue(DSL.field("timezone('utc'::text, now())", SQLDataType.LOCALDATETIME)), this, "");

    /**
     * The column <code>tapis_sys.systems_cred_info.has_credentials</code>.
     */
    public final TableField<SystemsCredInfoRecord, Boolean> HAS_CREDENTIALS = createField(DSL.name("has_credentials"), SQLDataType.BOOLEAN.nullable(false).defaultValue(DSL.field("false", SQLDataType.BOOLEAN)), this, "");

    /**
     * The column <code>tapis_sys.systems_cred_info.is_static</code>.
     */
    public final TableField<SystemsCredInfoRecord, Boolean> IS_STATIC = createField(DSL.name("is_static"), SQLDataType.BOOLEAN.nullable(false).defaultValue(DSL.field("false", SQLDataType.BOOLEAN)), this, "");

    /**
     * The column <code>tapis_sys.systems_cred_info.has_password</code>.
     */
    public final TableField<SystemsCredInfoRecord, Boolean> HAS_PASSWORD = createField(DSL.name("has_password"), SQLDataType.BOOLEAN.nullable(false).defaultValue(DSL.field("false", SQLDataType.BOOLEAN)), this, "");

    /**
     * The column <code>tapis_sys.systems_cred_info.has_pki_keys</code>.
     */
    public final TableField<SystemsCredInfoRecord, Boolean> HAS_PKI_KEYS = createField(DSL.name("has_pki_keys"), SQLDataType.BOOLEAN.nullable(false).defaultValue(DSL.field("false", SQLDataType.BOOLEAN)), this, "");

    /**
     * The column <code>tapis_sys.systems_cred_info.has_access_key</code>.
     */
    public final TableField<SystemsCredInfoRecord, Boolean> HAS_ACCESS_KEY = createField(DSL.name("has_access_key"), SQLDataType.BOOLEAN.nullable(false).defaultValue(DSL.field("false", SQLDataType.BOOLEAN)), this, "");

    /**
     * The column <code>tapis_sys.systems_cred_info.has_token</code>.
     */
    public final TableField<SystemsCredInfoRecord, Boolean> HAS_TOKEN = createField(DSL.name("has_token"), SQLDataType.BOOLEAN.nullable(false).defaultValue(DSL.field("false", SQLDataType.BOOLEAN)), this, "");

    /**
     * The column <code>tapis_sys.systems_cred_info.sync_status</code>.
     */
    public final TableField<SystemsCredInfoRecord, SyncStatus> SYNC_STATUS = createField(DSL.name("sync_status"), SQLDataType.CLOB.nullable(false).defaultValue(DSL.field("'PENDING'::text", SQLDataType.CLOB)), this, "", new EnumConverter<String, SyncStatus>(String.class, SyncStatus.class));

    /**
     * The column <code>tapis_sys.systems_cred_info.sync_failed</code>.
     */
    public final TableField<SystemsCredInfoRecord, LocalDateTime> SYNC_FAILED = createField(DSL.name("sync_failed"), SQLDataType.LOCALDATETIME(6), this, "");

    /**
     * The column <code>tapis_sys.systems_cred_info.sync_fail_count</code>.
     */
    public final TableField<SystemsCredInfoRecord, Integer> SYNC_FAIL_COUNT = createField(DSL.name("sync_fail_count"), SQLDataType.INTEGER.nullable(false).defaultValue(DSL.field("0", SQLDataType.INTEGER)), this, "");

    /**
     * The column <code>tapis_sys.systems_cred_info.sync_fail_message</code>.
     */
    public final TableField<SystemsCredInfoRecord, String> SYNC_FAIL_MESSAGE = createField(DSL.name("sync_fail_message"), SQLDataType.CLOB, this, "");

    private SystemsCredInfo(Name alias, Table<SystemsCredInfoRecord> aliased) {
        this(alias, aliased, null);
    }

    private SystemsCredInfo(Name alias, Table<SystemsCredInfoRecord> aliased, Field<?>[] parameters) {
        super(alias, null, aliased, parameters, DSL.comment(""), TableOptions.table());
    }

    /**
     * Create an aliased <code>tapis_sys.systems_cred_info</code> table
     * reference
     */
    public SystemsCredInfo(String alias) {
        this(DSL.name(alias), SYSTEMS_CRED_INFO);
    }

    /**
     * Create an aliased <code>tapis_sys.systems_cred_info</code> table
     * reference
     */
    public SystemsCredInfo(Name alias) {
        this(alias, SYSTEMS_CRED_INFO);
    }

    /**
     * Create a <code>tapis_sys.systems_cred_info</code> table reference
     */
    public SystemsCredInfo() {
        this(DSL.name("systems_cred_info"), null);
    }

    public <O extends Record> SystemsCredInfo(Table<O> child, ForeignKey<O, SystemsCredInfoRecord> key) {
        super(child, key, SYSTEMS_CRED_INFO);
    }

    @Override
    public Schema getSchema() {
        return aliased() ? null : TapisSys.TAPIS_SYS;
    }

    @Override
    public UniqueKey<SystemsCredInfoRecord> getPrimaryKey() {
        return Keys.SYSTEMS_CRED_INFO_PKEY;
    }

    @Override
    public List<ForeignKey<SystemsCredInfoRecord, ?>> getReferences() {
        return Arrays.asList(Keys.SYSTEMS_CRED_INFO__SYSTEMS_LOGIN_USER_SYSTEM_SEQ_ID_FKEY);
    }

    private transient Systems _systems;

    /**
     * Get the implicit join path to the <code>tapis_sys.systems</code> table.
     */
    public Systems systems() {
        if (_systems == null)
            _systems = new Systems(this, Keys.SYSTEMS_CRED_INFO__SYSTEMS_LOGIN_USER_SYSTEM_SEQ_ID_FKEY);

        return _systems;
    }

    @Override
    public SystemsCredInfo as(String alias) {
        return new SystemsCredInfo(DSL.name(alias), this);
    }

    @Override
    public SystemsCredInfo as(Name alias) {
        return new SystemsCredInfo(alias, this);
    }

    @Override
    public SystemsCredInfo as(Table<?> alias) {
        return new SystemsCredInfo(alias.getQualifiedName(), this);
    }

    /**
     * Rename this table
     */
    @Override
    public SystemsCredInfo rename(String name) {
        return new SystemsCredInfo(DSL.name(name), null);
    }

    /**
     * Rename this table
     */
    @Override
    public SystemsCredInfo rename(Name name) {
        return new SystemsCredInfo(name, null);
    }

    /**
     * Rename this table
     */
    @Override
    public SystemsCredInfo rename(Table<?> name) {
        return new SystemsCredInfo(name.getQualifiedName(), null);
    }

    // -------------------------------------------------------------------------
    // Row17 type methods
    // -------------------------------------------------------------------------

    @Override
    public Row17<Integer, String, String, String, String, LocalDateTime, LocalDateTime, Boolean, Boolean, Boolean, Boolean, Boolean, Boolean, SyncStatus, LocalDateTime, Integer, String> fieldsRow() {
        return (Row17) super.fieldsRow();
    }

    /**
     * Convenience mapping calling {@link SelectField#convertFrom(Function)}.
     */
    public <U> SelectField<U> mapping(Function17<? super Integer, ? super String, ? super String, ? super String, ? super String, ? super LocalDateTime, ? super LocalDateTime, ? super Boolean, ? super Boolean, ? super Boolean, ? super Boolean, ? super Boolean, ? super Boolean, ? super SyncStatus, ? super LocalDateTime, ? super Integer, ? super String, ? extends U> from) {
        return convertFrom(Records.mapping(from));
    }

    /**
     * Convenience mapping calling {@link SelectField#convertFrom(Class,
     * Function)}.
     */
    public <U> SelectField<U> mapping(Class<U> toType, Function17<? super Integer, ? super String, ? super String, ? super String, ? super String, ? super LocalDateTime, ? super LocalDateTime, ? super Boolean, ? super Boolean, ? super Boolean, ? super Boolean, ? super Boolean, ? super Boolean, ? super SyncStatus, ? super LocalDateTime, ? super Integer, ? super String, ? extends U> from) {
        return convertFrom(toType, Records.mapping(from));
    }
}
