/*
 * This file is generated by jOOQ.
 */
package edu.utexas.tacc.tapis.systems.gen.jooq.tables;


import edu.utexas.tacc.tapis.systems.gen.jooq.Keys;
import edu.utexas.tacc.tapis.systems.gen.jooq.TapisSys;
import edu.utexas.tacc.tapis.systems.gen.jooq.tables.records.SchedProfileModLoadRecord;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.jooq.Field;
import org.jooq.ForeignKey;
import org.jooq.Function6;
import org.jooq.Identity;
import org.jooq.Name;
import org.jooq.Record;
import org.jooq.Records;
import org.jooq.Row6;
import org.jooq.Schema;
import org.jooq.SelectField;
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
public class SchedProfileModLoad extends TableImpl<SchedProfileModLoadRecord> {

    private static final long serialVersionUID = 1L;

    /**
     * The reference instance of <code>tapis_sys.sched_profile_mod_load</code>
     */
    public static final SchedProfileModLoad SCHED_PROFILE_MOD_LOAD = new SchedProfileModLoad();

    /**
     * The class holding records for this type
     */
    @Override
    public Class<SchedProfileModLoadRecord> getRecordType() {
        return SchedProfileModLoadRecord.class;
    }

    /**
     * The column <code>tapis_sys.sched_profile_mod_load.seq_id</code>.
     */
    public final TableField<SchedProfileModLoadRecord, Integer> SEQ_ID = createField(DSL.name("seq_id"), SQLDataType.INTEGER.nullable(false).identity(true), this, "");

    /**
     * The column
     * <code>tapis_sys.sched_profile_mod_load.sched_profile_seq_id</code>.
     */
    public final TableField<SchedProfileModLoadRecord, Integer> SCHED_PROFILE_SEQ_ID = createField(DSL.name("sched_profile_seq_id"), SQLDataType.INTEGER, this, "");

    /**
     * The column
     * <code>tapis_sys.sched_profile_mod_load.sched_profile_name</code>.
     */
    public final TableField<SchedProfileModLoadRecord, String> SCHED_PROFILE_NAME = createField(DSL.name("sched_profile_name"), SQLDataType.CLOB, this, "");

    /**
     * The column <code>tapis_sys.sched_profile_mod_load.tenant</code>.
     */
    public final TableField<SchedProfileModLoadRecord, String> TENANT = createField(DSL.name("tenant"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column
     * <code>tapis_sys.sched_profile_mod_load.module_load_command</code>.
     */
    public final TableField<SchedProfileModLoadRecord, String> MODULE_LOAD_COMMAND = createField(DSL.name("module_load_command"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>tapis_sys.sched_profile_mod_load.modules_to_load</code>.
     */
    public final TableField<SchedProfileModLoadRecord, String[]> MODULES_TO_LOAD = createField(DSL.name("modules_to_load"), SQLDataType.CLOB.getArrayDataType(), this, "");

    private SchedProfileModLoad(Name alias, Table<SchedProfileModLoadRecord> aliased) {
        this(alias, aliased, null);
    }

    private SchedProfileModLoad(Name alias, Table<SchedProfileModLoadRecord> aliased, Field<?>[] parameters) {
        super(alias, null, aliased, parameters, DSL.comment(""), TableOptions.table());
    }

    /**
     * Create an aliased <code>tapis_sys.sched_profile_mod_load</code> table
     * reference
     */
    public SchedProfileModLoad(String alias) {
        this(DSL.name(alias), SCHED_PROFILE_MOD_LOAD);
    }

    /**
     * Create an aliased <code>tapis_sys.sched_profile_mod_load</code> table
     * reference
     */
    public SchedProfileModLoad(Name alias) {
        this(alias, SCHED_PROFILE_MOD_LOAD);
    }

    /**
     * Create a <code>tapis_sys.sched_profile_mod_load</code> table reference
     */
    public SchedProfileModLoad() {
        this(DSL.name("sched_profile_mod_load"), null);
    }

    public <O extends Record> SchedProfileModLoad(Table<O> child, ForeignKey<O, SchedProfileModLoadRecord> key) {
        super(child, key, SCHED_PROFILE_MOD_LOAD);
    }

    @Override
    public Schema getSchema() {
        return aliased() ? null : TapisSys.TAPIS_SYS;
    }

    @Override
    public Identity<SchedProfileModLoadRecord, Integer> getIdentity() {
        return (Identity<SchedProfileModLoadRecord, Integer>) super.getIdentity();
    }

    @Override
    public UniqueKey<SchedProfileModLoadRecord> getPrimaryKey() {
        return Keys.SCHED_PROFILE_MOD_LOAD_PKEY;
    }

    @Override
    public List<ForeignKey<SchedProfileModLoadRecord, ?>> getReferences() {
        return Arrays.asList(Keys.SCHED_PROFILE_MOD_LOAD__SCHED_PROFILE_MOD_LOAD_SCHED_PROFILE_SEQ_ID_FKEY);
    }

    private transient SchedulerProfiles _schedulerProfiles;

    /**
     * Get the implicit join path to the
     * <code>tapis_sys.scheduler_profiles</code> table.
     */
    public SchedulerProfiles schedulerProfiles() {
        if (_schedulerProfiles == null)
            _schedulerProfiles = new SchedulerProfiles(this, Keys.SCHED_PROFILE_MOD_LOAD__SCHED_PROFILE_MOD_LOAD_SCHED_PROFILE_SEQ_ID_FKEY);

        return _schedulerProfiles;
    }

    @Override
    public SchedProfileModLoad as(String alias) {
        return new SchedProfileModLoad(DSL.name(alias), this);
    }

    @Override
    public SchedProfileModLoad as(Name alias) {
        return new SchedProfileModLoad(alias, this);
    }

    @Override
    public SchedProfileModLoad as(Table<?> alias) {
        return new SchedProfileModLoad(alias.getQualifiedName(), this);
    }

    /**
     * Rename this table
     */
    @Override
    public SchedProfileModLoad rename(String name) {
        return new SchedProfileModLoad(DSL.name(name), null);
    }

    /**
     * Rename this table
     */
    @Override
    public SchedProfileModLoad rename(Name name) {
        return new SchedProfileModLoad(name, null);
    }

    /**
     * Rename this table
     */
    @Override
    public SchedProfileModLoad rename(Table<?> name) {
        return new SchedProfileModLoad(name.getQualifiedName(), null);
    }

    // -------------------------------------------------------------------------
    // Row6 type methods
    // -------------------------------------------------------------------------

    @Override
    public Row6<Integer, Integer, String, String, String, String[]> fieldsRow() {
        return (Row6) super.fieldsRow();
    }

    /**
     * Convenience mapping calling {@link SelectField#convertFrom(Function)}.
     */
    public <U> SelectField<U> mapping(Function6<? super Integer, ? super Integer, ? super String, ? super String, ? super String, ? super String[], ? extends U> from) {
        return convertFrom(Records.mapping(from));
    }

    /**
     * Convenience mapping calling {@link SelectField#convertFrom(Class,
     * Function)}.
     */
    public <U> SelectField<U> mapping(Class<U> toType, Function6<? super Integer, ? super Integer, ? super String, ? super String, ? super String, ? super String[], ? extends U> from) {
        return convertFrom(toType, Records.mapping(from));
    }
}
