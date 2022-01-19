package flink.examples.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class HbaseDB {

    private static transient Connection connection = null;
    private static transient HTable table = null;

    private volatile static HbaseDB instance;

    private HbaseDB(){}

    public static HbaseDB create(Map<String,String> config) throws IOException {
        if(instance == null){
            synchronized (HbaseDB.class){
                if(instance == null){
                    instance = new HbaseDB();
                }
            }
        }
        open(config);
        return instance;
    }


    public static void open(Map<String,String> config) throws IOException {
        Configuration hbaseConf = HBaseOptions.getHBaseConf(config);
        String tableName = config.get(HBaseOptions.TABLE_NAME);
        byte[] serializedConfig = HBaseConfigurationUtil.serializeConfiguration(hbaseConf);
        org.apache.hadoop.conf.Configuration runtimeConfig = HBaseConfigurationUtil.deserializeConfiguration(
                serializedConfig, HBaseConfigurationUtil.getHBaseConfiguration());
        connection = ConnectionFactory.createConnection(runtimeConfig);
        table = (HTable) connection.getTable(TableName.valueOf(tableName));
    }



    public void delete(byte[] family,byte[] qualifier,byte[] row) throws IOException {
        Delete del = new Delete(row);
        del.addColumn(family, qualifier);
        table.delete(del);
    }

    public ResultScanner scan(byte[] family, byte[] qualifier, byte[] prefixBytes) throws IOException {
        Scan s = new Scan();
        s.addColumn(family, qualifier);
        s.setFilter(new PrefixFilter(prefixBytes));
        ResultScanner rs = table.getScanner(s);
        return rs;
    }

    public void muiltPut(List<Put> puts) throws IOException {
        table.put(puts);
    }

    public void put(Put put) throws IOException {
        table.put(put);
    }

    public Result get(byte[] family, byte[] qualifier, byte[] row) throws IOException {
        Get get = new Get(row);
        get.addColumn(family,qualifier);
        return table.get(get);
    }

/*    public void connect() throws IOException {
        Admin admin = connection.getAdmin();
        admin.
    }*/
}
