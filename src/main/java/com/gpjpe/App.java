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
                "Query 2: ${APP}/hbaseApp.sh 2 startTS endTS N lang-1,lang-2,lang-3 outputFolder\n" +
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
        
        long startTS;
        long endTS;
        String outputFolder;
        int topN;
        TopQuery query;

        switch (mode){
            case 1:
            	
            	startTS = Long.parseLong(args[1]);
            	endTS = Long.parseLong(args[2]);
            	topN = Integer.parseInt(args[3]);
            	String language = args[4];
            	outputFolder = args[5];
            	
            	connection = HConnectionManager.createConnection(configuration);
            	query = new TopQuery(connection);
            	
            	query.topHashTagsForLanguageWithinTimRange(language, topN, startTS, endTS, outputFolder, ID);
            	
                break;
                
            case 2:
            	
            	startTS = Long.parseLong(args[1]);
            	endTS = Long.parseLong(args[2]);
            	topN = Integer.parseInt(args[3]);
            	String[] languages = args[4].split(",");
            	outputFolder = args[5];
            	
            	connection = HConnectionManager.createConnection(configuration);
            	query = new TopQuery(connection);
            	
            	query.topHashTagsForLanguagesInTimeRange(languages, topN, startTS, endTS, outputFolder, ID);            	
            	
                break;
                
            case 3:
            	
            	startTS = Long.parseLong(args[1]);
            	endTS = Long.parseLong(args[2]);
            	topN = Integer.parseInt(args[3]);
            	outputFolder = args[4];
            	
            	connection = HConnectionManager.createConnection(configuration);
            	query = new TopQuery(connection);
            	
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
