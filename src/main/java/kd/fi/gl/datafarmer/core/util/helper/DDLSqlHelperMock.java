package kd.fi.gl.datafarmer.core.util.helper;

public class DDLSqlHelperMock extends DDLSqlHelper {


    @Override
    public int createIndex(boolean unique, String indexName, String tableName, String columns) {
        return 1;
    }

    @Override
    public int addPrimaryKey(String tableName, String column) {
        return 1;
    }


}
