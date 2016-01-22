package com.gpjpe;

import com.gpjpe.domain.TweetsHTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;


public class App {

    private final static Logger LOGGER = LoggerFactory.getLogger(App.class.getName());

    public static void usage(){

        LOGGER.info(("\nLoad   : ${APP}/hbaseApp.sh mode dataFolder\n" +
                "Query 1: ${APP}/hbaseApp.sh mode startTS endTS N language outputFolder\n" +
                "Query 2: ${APP}/hbaseApp.sh mode startTS endTS N language outputFolde\n" +
                "Query 3: ${APP}/hbaseApp.sh mode startTS endTS N outputFolder"));
    }

    public static void main(String[] args) throws IOException {

        LOGGER.info(String.format("Received %d arguments", args.length));

        int mode = Integer.parseInt(args[0]);

        if (Arrays.asList(new String[]{"2", "5", "6"}).contains(Integer.toString(args.length))){
            usage();
            throw new RuntimeException("Invalid number of arguments");
        }

        switch (mode){
            case 1:
                break;
            case 2:
                break;
            case 3:
                break;
            case 4:
                break;
            default:
                usage();
                throw new RuntimeException(String.format("Unknown mode [%d]", mode));
        }

        Configuration configuration = HBaseConfiguration.create();

        TweetsHTable tweetsHTable = new TweetsHTable(configuration);

        tweetsHTable.initializeSchema();
    }
}
