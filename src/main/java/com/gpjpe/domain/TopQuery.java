package com.gpjpe.domain;
import com.gpjpe.helpers.HashtagCountComparator;
import com.gpjpe.helpers.Utils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;


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

    public void query1(String lang, int topN, long startTimestamp, long endTimestamp, String outputFolder, String outPrefix) {
        Map<String, Integer> hashtagCountMap = new HashMap<>();
        List<HashtagCount> hashtagCountList = new ArrayList<>();

        // Build Scanner and Filter
        Scan scan = new Scan(
                Utils.generateKey(startTimestamp, lang),
                Utils.generateKey(endTimestamp, lang));
        Filter f = new SingleColumnValueFilter(
                Bytes.toBytes(Schema.CF_META),
                Bytes.toBytes(Schema.COLUMN_META_LANG),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes(lang));
        scan.setFilter(f);

        //Fill hashtagCountMap
        try {
            ResultScanner resultScanner = table.getScanner(scan);
            Result result = resultScanner.next();

            while (result != null && !result.isEmpty()) {
                for (Map.Entry<byte[], byte[]> entry : result.getFamilyMap(Bytes.toBytes(Schema.CF_HT)).entrySet()) {
                    hashtagCountMap.put(Bytes.toString(entry.getKey()), Bytes.toInt(entry.getValue()));
                }

                result = resultScanner.next();
            }

        } catch (IOException e) {
            LOGGER.error(String.valueOf(e));
            throw new RuntimeException(e);
        }
        //Write Results to file
        for (String hashtag : hashtagCountMap.keySet()) {
            hashtagCountList.add(new HashtagCount(hashtag, hashtagCountMap.get(hashtag)));
        }

        Collections.sort(hashtagCountList, new HashtagCountComparator());
        Collections.reverse(hashtagCountList);

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < topN; i++){
            if (i < hashtagCountList.size()) {
                sb.append(lang)
                        .append(i+1)
                        .append(hashtagCountList.get(i).getHashtag())
                        .append(startTimestamp)
                        .append(endTimestamp);

            } else {
                sb.append(lang)
                        .append(i+1)
                        .append("null")
                        .append(startTimestamp)
                        .append(endTimestamp);
            }
            sb.append("\n");
        }

        Utils.writeToFile(sb.toString(),outputFolder,1,outPrefix);

    }

    public void query2(String[] langs, int topN, long startTimestamp, long endTimestamp, String outputFolder, String outPrefix) {
        Map<String,Map<String, Integer>> hashtagCountMapMap = new HashMap<>();
        Map<String,List<HashtagCount>>hashtagCountListMap = new HashMap<>();

        for(String lang : langs){
            hashtagCountMapMap.put(lang,new HashMap<String, Integer>());
        }

        for(String lang : langs){
            hashtagCountListMap.put(lang,new ArrayList<HashtagCount>());
        }

        // Build Scanner and Filter
        Scan scan = new Scan(
                Utils.generateKey(startTimestamp, Utils.QueryKey.start),
                Utils.generateKey(endTimestamp, Utils.QueryKey.end));
        FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ONE);

        for (String lang : langs) {
            filters.addFilter(new SingleColumnValueFilter(
                    Bytes.toBytes(Schema.CF_META),
                    Bytes.toBytes(Schema.COLUMN_META_LANG),
                    CompareFilter.CompareOp.EQUAL,
                    Bytes.toBytes(lang)));
        }
        scan.setFilter(filters);

        //Fill HashtagCounMap
        try {
            ResultScanner resultScanner = table.getScanner(scan);
            Result result = resultScanner.next();

            while (result != null && !result.isEmpty()) {
                for (Map.Entry<byte[], byte[]> entry : result.getFamilyMap(Bytes.toBytes(Schema.CF_HT)).entrySet()) {
                    hashtagCountMapMap
                            .get(Bytes.toString(result.getValue(Bytes.toBytes(Schema.CF_META),Bytes.toBytes(Schema.COLUMN_META_LANG))))
                            .put(Bytes.toString(entry.getKey()), Bytes.toInt(entry.getValue()));
                }

                result = resultScanner.next();
            }

        } catch (IOException e) {
            LOGGER.error(String.valueOf(e));
            throw new RuntimeException(e);
        }

        StringBuilder sb = new StringBuilder();

        //Write results to File
        for (String lang : hashtagCountMapMap.keySet()){
            for (String hashtag : hashtagCountMapMap.get(lang).keySet()) {
                hashtagCountListMap
                        .get(lang)
                        .add(new HashtagCount(hashtag, hashtagCountMapMap.get(lang).get(hashtag)));
            }

            Collections.sort(hashtagCountListMap.get(lang), new HashtagCountComparator());
            Collections.reverse(hashtagCountListMap.get(lang));

            for (int i = 0; i < topN; i++){
                if (i < hashtagCountListMap.get(lang).size()) {
                    sb.append(lang)
                            .append(i+1)
                            .append(hashtagCountListMap.get(lang).get(i).getHashtag())
                            .append(startTimestamp)
                            .append(endTimestamp);

                } else {
                    sb.append(lang)
                            .append(i+1)
                            .append("null")
                            .append(startTimestamp)
                            .append(endTimestamp);
                }
                sb.append("\n");
            }

        }
        Utils.writeToFile(sb.toString(),outputFolder,2,outPrefix);

    }
}
