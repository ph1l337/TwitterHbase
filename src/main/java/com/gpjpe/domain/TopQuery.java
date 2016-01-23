package com.gpjpe.domain;

import com.gpjpe.helpers.Utils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class TopQuery {
    private static Logger LOGGER = LoggerFactory.getLogger(TopQuery.class.getName());
    private HTable table;


    public TopQuery(HConnection conn) {
        try {
            this.table = new HTable(TableName.valueOf(Schema.TABLE_NAME), conn);
        } catch (IOException e) {
            LOGGER.error(String.valueOf(e));
            throw new RuntimeException(e);
        }

    }

    public void topNForLang(String lang, int n, long startTimestamp, long endTimestamp) {
        Map<String, Integer> hashTags = new HashMap<>();
        Scan scan = new Scan(
                Utils.generateKey(startTimestamp, lang),
                Utils.generateKey(endTimestamp, lang));
        Filter f = new SingleColumnValueFilter(
                Bytes.toBytes(Schema.CF_META),
                Bytes.toBytes(Schema.COLUMN_META_LANG),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes(lang));
        scan.setFilter(f);

        try {
            ResultScanner resultScanner = table.getScanner(scan);
            Result result = resultScanner.next();

            while (result != null && !result.isEmpty()) {
                for (Map.Entry<byte[], byte[]> entry : result.getFamilyMap(Bytes.toBytes(Schema.CF_HT)).entrySet()) {
                    hashTags.put(Bytes.toString(entry.getKey()), Bytes.toInt(entry.getValue()));
                }

                result = resultScanner.next();
            }

        } catch (IOException e) {
            LOGGER.error(String.valueOf(e));
            throw new RuntimeException(e);
        }
    }


}
