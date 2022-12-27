package org.apache.flink.connector.clickhouse.internal.common;

public class TableFullSchema {
    private String engine;
    private String cluster;
    private String database;
    private String table;
    private boolean distributed;

    public TableFullSchema(String engine, String database, String table) {
        this.engine = engine;
        this.database = database;
        this.table = table;
    }
    

    public String getEngine() {
        return engine;
    }

    public void setEngine(String engine) {
        this.engine = engine;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public boolean isDistributed() {
        return distributed;
    }

    public void setDistributed(boolean distributed) {
        this.distributed = distributed;
    }
}
