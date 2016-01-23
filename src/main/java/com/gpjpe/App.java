package com.gpjpe;

import com.gpjpe.domain.TopQuery;
import com.gpjpe.domain.TweetsHTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;


public class App {

    private final static Logger LOGGER = LoggerFactory.getLogger(App.class.getName());
    private final static String ID = "05";

    public static void usage(){

        LOGGER.info(("\nLoad   : ${APP}/hbaseApp.sh mode dataFolder\n" +
                "Query 1: ${APP}/hbaseApp.sh 1 startTS endTS N language outputFolder\n" +
                "Query 2: ${APP}/hbaseApp.sh 2 startTS endTS N language outputFolde\n" +
                "Query 3: ${APP}/hbaseApp.sh 3 startTS endTS N outputFolder"));
    }

    public static void main(String[] args) throws IOException {

        LOGGER.info(String.format("Received %d arguments", args.length));
        
        if (args.length < 2) {
        	usage();
        	throw new RuntimeException("Invalid number of arguments");
        }
        
        int mode = Integer.parseInt(args[0]);

        if (!Arrays.asList(new String[]{"2", "5", "6"}).contains(Integer.toString(args.length))){
            usage();
            throw new RuntimeException("Invalid number of arguments");
        }
        
        Configuration configuration = HBaseConfiguration.create();
        HConnection connection;

        switch (mode){
            case 1:
                break;
            case 2:
                break;
            case 3:
            	
            	long startTS = Long.parseLong(args[1]);
            	long endTS = Long.parseLong(args[2]);
            	int topN = Integer.parseInt(args[3]);
            	String outputFolder = args[4];
            	
            	connection = HConnectionManager.createConnection(configuration);
            	TopQuery query = new TopQuery(connection);
            	
            	query.topHashTagsInTimeRange(topN, startTS, endTS, outputFolder, ID);
            	
                break;
                
            case 4:
            	
            	String filePath = args[1];

                TweetsHTable tweetsHTable = new TweetsHTable(configuration);

                tweetsHTable.initializeSchema(); 
                
                tweetsHTable.insertRecords(filePath);
                
                break;
            default:
                usage();
                throw new RuntimeException(String.format("Unknown mode [%d]", mode));
        }
    }
}
