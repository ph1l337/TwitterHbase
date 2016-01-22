package com.gpjpe.domain;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class TopQuery {
    private static Logger LOGGER = LoggerFactory.getLogger(TopQuery.class.getName());
    private HTable table;


    public TopQuery(HConnection conn){
        try {
            this.table = new HTable(TableName.valueOf(Schema.TABLE_NAME),conn);
        } catch (IOException e) {
            LOGGER.error(String.valueOf(e));
            throw new RuntimeException(e);
        }

    }

    public void topNForLang(String lang, int n, long startTimestamp, long endTimestamp){
        Scan scan = new Scan ();
        Filter f = new SingleColumnValueFilter(
                            Bytes.toBytes(Schema.CF_META),
                            Bytes.toBytes(Schema.COLUMN_HT_LANG),
                            CompareFilter.CompareOp.EQUAL,
                            Bytes.toBytes(lang));
        scan.setFilter(f);
    }


}
