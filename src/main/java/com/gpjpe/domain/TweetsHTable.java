package com.gpjpe.domain;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class TweetsHTable {

    private final static Logger LOGGER = LoggerFactory.getLogger(TweetsHTable.class.getName());


    private Configuration conf;

    public TweetsHTable(Configuration conf) {
        this.conf = conf;
    }

    public void initializeSchema() throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);

        byte[] tableName = Bytes.toBytes(Schema.TABLE_NAME);

        for(String columnFamily: new String[]{Schema.CF_HT, Schema.CF_META}){
            byte[] CF = Bytes.toBytes(columnFamily);
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor family = new HColumnDescriptor(CF);
            tableDescriptor.addFamily(family);

            if (admin.tableExists(Bytes.toBytes(Bytes.toString(tableDescriptor.getName())))){
                LOGGER.info("Dropping table " + Bytes.toString(tableDescriptor.getName()));
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }

            LOGGER.info("Creating table " + Bytes.toString(tableDescriptor.getName()));
            admin.createTable(tableDescriptor);
        }
    }
}
