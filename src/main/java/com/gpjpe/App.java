package com.gpjpe;

import com.gpjpe.domain.TweetsHTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class App {

    private final static Logger LOGGER = LoggerFactory.getLogger(App.class.getName());

    public static void main(String[] args) throws IOException {

        Configuration configuration = HBaseConfiguration.create();

      //  TweetsHTable tweetsHTable = new TweetsHTable(configuration);

       // tweetsHTable.initializeSchema();

    }
}
