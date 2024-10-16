/*
 * This file is generated by jOOQ.
 */
package edu.utexas.tacc.tapis.systems.gen.jooq.tables;


import com.google.gson.JsonElement;

import edu.utexas.tacc.tapis.systems.dao.JSONBToJsonElementBinding;
import edu.utexas.tacc.tapis.systems.gen.jooq.Keys;
import edu.utexas.tacc.tapis.systems.gen.jooq.TapisSys;
import edu.utexas.tacc.tapis.systems.gen.jooq.tables.records.SystemUpdatesRecord;
import edu.utexas.tacc.tapis.systems.model.TSystem.SystemOperation;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.jooq.Field;
import org.jooq.ForeignKey;
import org.jooq.Function12;
import org.jooq.Identity;
import org.jooq.Name;
import org.jooq.Record;
import org.jooq.Records;
import org.jooq.Row12;
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
public class SystemUpdates extends TableImpl<SystemUpdatesRecord> {

    private static final long serialVersionUID = 1L;

    /**
     * The reference instance of <code>tapis_sys.system_updates</code>
     */
    public static final SystemUpdates SYSTEM_UPDATES = new SystemUpdates();

    /**
     * The class holding records for this type
     */
    @Override
    public Class<SystemUpdatesRecord> getRecordType() {
        return SystemUpdatesRecord.class;
    }

    /**
     * The column <code>tapis_sys.system_updates.seq_id</code>. System update
     * request sequence id
     */
    public final TableField<SystemUpdatesRecord, Integer> SEQ_ID = createField(DSL.name("seq_id"), SQLDataType.INTEGER.nullable(false).identity(true), this, "System update request sequence id");

    /**
     * The column <code>tapis_sys.system_updates.system_seq_id</code>. Sequence
     * id of system being updated
     */
    public final TableField<SystemUpdatesRecord, Integer> SYSTEM_SEQ_ID = createField(DSL.name("system_seq_id"), SQLDataType.INTEGER, this, "Sequence id of system being updated");

    /**
     * The column <code>tapis_sys.system_updates.obo_tenant</code>. OBO Tenant
     * associated with the change request
     */
    public final TableField<SystemUpdatesRecord, String> OBO_TENANT = createField(DSL.name("obo_tenant"), SQLDataType.CLOB.nullable(false), this, "OBO Tenant associated with the change request");

    /**
     * The column <code>tapis_sys.system_updates.obo_user</code>. OBO User
     * associated with the change request
     */
    public final TableField<SystemUpdatesRecord, String> OBO_USER = createField(DSL.name("obo_user"), SQLDataType.CLOB.nullable(false), this, "OBO User associated with the change request");

    /**
     * The column <code>tapis_sys.system_updates.jwt_tenant</code>. Tenant of
     * user who requested the update
     */
    public final TableField<SystemUpdatesRecord, String> JWT_TENANT = createField(DSL.name("jwt_tenant"), SQLDataType.CLOB.nullable(false), this, "Tenant of user who requested the update");

    /**
     * The column <code>tapis_sys.system_updates.jwt_user</code>. Name of user
     * who requested the update
     */
    public final TableField<SystemUpdatesRecord, String> JWT_USER = createField(DSL.name("jwt_user"), SQLDataType.CLOB.nullable(false), this, "Name of user who requested the update");

    /**
     * The column <code>tapis_sys.system_updates.system_id</code>. Id of system
     * being updated
     */
    public final TableField<SystemUpdatesRecord, String> SYSTEM_ID = createField(DSL.name("system_id"), SQLDataType.CLOB.nullable(false), this, "Id of system being updated");

    /**
     * The column <code>tapis_sys.system_updates.operation</code>. Type of
     * update operation
     */
    public final TableField<SystemUpdatesRecord, SystemOperation> OPERATION = createField(DSL.name("operation"), SQLDataType.CLOB.nullable(false), this, "Type of update operation", new EnumConverter<String, SystemOperation>(String.class, SystemOperation.class));

    /**
     * The column <code>tapis_sys.system_updates.description</code>. JSON
     * describing the change. Secrets scrubbed as needed.
     */
    public final TableField<SystemUpdatesRecord, JsonElement> DESCRIPTION = createField(DSL.name("description"), SQLDataType.JSONB.nullable(false), this, "JSON describing the change. Secrets scrubbed as needed.", new JSONBToJsonElementBinding());

    /**
     * The column <code>tapis_sys.system_updates.raw_data</code>. Raw data
     * associated with the request, if available. Secrets scrubbed as needed.
     */
    public final TableField<SystemUpdatesRecord, String> RAW_DATA = createField(DSL.name("raw_data"), SQLDataType.CLOB, this, "Raw data associated with the request, if available. Secrets scrubbed as needed.");

    /**
     * The column <code>tapis_sys.system_updates.uuid</code>. UUID of system
     * being updated
     */
    public final TableField<SystemUpdatesRecord, java.util.UUID> UUID = createField(DSL.name("uuid"), SQLDataType.UUID.nullable(false), this, "UUID of system being updated");

    /**
     * The column <code>tapis_sys.system_updates.created</code>. UTC time for
     * when record was created
     */
    public final TableField<SystemUpdatesRecord, LocalDateTime> CREATED = createField(DSL.name("created"), SQLDataType.LOCALDATETIME(6).nullable(false).defaultValue(DSL.field("timezone('utc'::text, now())", SQLDataType.LOCALDATETIME)), this, "UTC time for when record was created");

    private SystemUpdates(Name alias, Table<SystemUpdatesRecord> aliased) {
        this(alias, aliased, null);
    }

    private SystemUpdates(Name alias, Table<SystemUpdatesRecord> aliased, Field<?>[] parameters) {
        super(alias, null, aliased, parameters, DSL.comment(""), TableOptions.table());
    }

    /**
     * Create an aliased <code>tapis_sys.system_updates</code> table reference
     */
    public SystemUpdates(String alias) {
        this(DSL.name(alias), SYSTEM_UPDATES);
    }

    /**
     * Create an aliased <code>tapis_sys.system_updates</code> table reference
     */
    public SystemUpdates(Name alias) {
        this(alias, SYSTEM_UPDATES);
    }

    /**
     * Create a <code>tapis_sys.system_updates</code> table reference
     */
    public SystemUpdates() {
        this(DSL.name("system_updates"), null);
    }

    public <O extends Record> SystemUpdates(Table<O> child, ForeignKey<O, SystemUpdatesRecord> key) {
        super(child, key, SYSTEM_UPDATES);
    }

    @Override
    public Schema getSchema() {
        return aliased() ? null : TapisSys.TAPIS_SYS;
    }

    @Override
    public Identity<SystemUpdatesRecord, Integer> getIdentity() {
        return (Identity<SystemUpdatesRecord, Integer>) super.getIdentity();
    }

    @Override
    public UniqueKey<SystemUpdatesRecord> getPrimaryKey() {
        return Keys.SYSTEM_UPDATES_PKEY;
    }

    @Override
    public List<ForeignKey<SystemUpdatesRecord, ?>> getReferences() {
        return Arrays.asList(Keys.SYSTEM_UPDATES__SYSTEM_UPDATES_SYSTEM_SEQ_ID_FKEY);
    }

    private transient Systems _systems;

    /**
     * Get the implicit join path to the <code>tapis_sys.systems</code> table.
     */
    public Systems systems() {
        if (_systems == null)
            _systems = new Systems(this, Keys.SYSTEM_UPDATES__SYSTEM_UPDATES_SYSTEM_SEQ_ID_FKEY);

        return _systems;
    }

    @Override
    public SystemUpdates as(String alias) {
        return new SystemUpdates(DSL.name(alias), this);
    }

    @Override
    public SystemUpdates as(Name alias) {
        return new SystemUpdates(alias, this);
    }

    @Override
    public SystemUpdates as(Table<?> alias) {
        return new SystemUpdates(alias.getQualifiedName(), this);
    }

    /**
     * Rename this table
     */
    @Override
    public SystemUpdates rename(String name) {
        return new SystemUpdates(DSL.name(name), null);
    }

    /**
     * Rename this table
     */
    @Override
    public SystemUpdates rename(Name name) {
        return new SystemUpdates(name, null);
    }

    /**
     * Rename this table
     */
    @Override
    public SystemUpdates rename(Table<?> name) {
        return new SystemUpdates(name.getQualifiedName(), null);
    }

    // -------------------------------------------------------------------------
    // Row12 type methods
    // -------------------------------------------------------------------------

    @Override
    public Row12<Integer, Integer, String, String, String, String, String, SystemOperation, JsonElement, String, java.util.UUID, LocalDateTime> fieldsRow() {
        return (Row12) super.fieldsRow();
    }

    /**
     * Convenience mapping calling {@link SelectField#convertFrom(Function)}.
     */
    public <U> SelectField<U> mapping(Function12<? super Integer, ? super Integer, ? super String, ? super String, ? super String, ? super String, ? super String, ? super SystemOperation, ? super JsonElement, ? super String, ? super java.util.UUID, ? super LocalDateTime, ? extends U> from) {
        return convertFrom(Records.mapping(from));
    }

    /**
     * Convenience mapping calling {@link SelectField#convertFrom(Class,
     * Function)}.
     */
    public <U> SelectField<U> mapping(Class<U> toType, Function12<? super Integer, ? super Integer, ? super String, ? super String, ? super String, ? super String, ? super String, ? super SystemOperation, ? super JsonElement, ? super String, ? super java.util.UUID, ? super LocalDateTime, ? extends U> from) {
        return convertFrom(toType, Records.mapping(from));
    }
}
