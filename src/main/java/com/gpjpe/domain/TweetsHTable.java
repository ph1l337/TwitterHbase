package com.gpjpe.domain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gpjpe.helpers.Utils;

public class TweetsHTable {

	private final static Logger LOGGER = LoggerFactory
			.getLogger(TweetsHTable.class.getName());

	private Configuration conf;

	public TweetsHTable(Configuration conf) {
		this.conf = conf;
	}

	public void initializeSchema() throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);

		byte[] tableName = Bytes.toBytes(Schema.TABLE_NAME);
		byte[] CF;

		HTableDescriptor tableDescriptor = new HTableDescriptor(
				TableName.valueOf(tableName));
		
		for (String columnFamily : new String[] { Schema.CF_HT, Schema.CF_META }) {
			
			CF = Bytes.toBytes(columnFamily);
			
			HColumnDescriptor family = new HColumnDescriptor(CF);
			tableDescriptor.addFamily(family);

			if (admin.tableExists(Bytes.toBytes(Bytes.toString(tableDescriptor
					.getName())))) {
				LOGGER.info("Dropping table "
						+ Bytes.toString(tableDescriptor.getName()));
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}

			LOGGER.info("Creating table "
					+ Bytes.toString(tableDescriptor.getName()));
			admin.createTable(tableDescriptor);
		}

		admin.close();
	}

	public void insertRecords(String filePath) throws IOException {

		IWindowSummaryReader summaryReader = new WindowSummaryFileReader(filePath);
		WindowSummary windowSummary;
		byte[] CF;
		byte[] rowKey;
		Put put;
		HTable table = new HTable(this.conf, Bytes.toBytes(Schema.TABLE_NAME));
		List<Put> puts = new ArrayList<Put>();
		long batchSize = 1000;
		long count = 0;
		
		while ((windowSummary = summaryReader.next()) != null) {
			
			//row key
			CF = Bytes.toBytes(Schema.CF_HT);
			rowKey = Utils.generateKey(windowSummary.getWindow(), windowSummary.getLanguage());
			
			put = new Put(rowKey);
			
			//hash tags
			for(Map.Entry<String, Integer> entry: windowSummary.getHashTagCountMap().entrySet()) {
				put.add(CF, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
			}
			
			CF = Bytes.toBytes(Schema.CF_META);
			
			put.add(CF, Bytes.toBytes(Schema.COLUMN_META_LANG), Bytes.toBytes(windowSummary.getLanguage()));
			puts.add(put);
			
			if (count % batchSize == 0) {
				table.put(puts);
				table.flushCommits();
				puts.clear();
			}
			
			count++;
		}
		
		if (puts.size() > 0) {
			table.put(puts);
			table.flushCommits();
			puts.clear();			
		}
		
		LOGGER.info(String.format("Inserted %d records", count));

		table.close();
	}
}
