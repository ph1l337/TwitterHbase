package com.gpjpe.helpers;


import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class Utils {
    private static Logger LOGGER = LoggerFactory.getLogger(Utils.class.getName());
    public enum QueryKey {start, end}
    public static byte[] generateKey(long timeStamp, String lang){
        byte[] key = new byte[10];
        System.arraycopy(Bytes.toBytes(timeStamp),0,key,0,8);
        System.arraycopy(Bytes.toBytes(lang),0,key,8,2);
        return key;
    }
    public static byte[] generateKey(long timeStamp, QueryKey queryKey){
        byte[] key = new byte[10];
        System.arraycopy(Bytes.toBytes(timeStamp),0,key,0,8);
        switch (queryKey) {
            case start:
                byte[] low = {(byte) -255, (byte) -255};
                System.arraycopy(low,0,key,8,2);
                break;
            case end:
                byte[] high = {(byte) 255, (byte) 255};
                System.arraycopy(high,0,key,8,2);
                break;
        }
        return key;
    }

    public static void writeToFile(String ouput, String outputFolder, int queryId, String outPrefix){
        BufferedWriter writer = null;
        try {

            File dir = new File(outputFolder);
            if (!dir.exists()) {
                if (!dir.mkdirs()) {
                    if (!dir.exists()) {
                        throw new RuntimeException(
                                String.format("Couldn't create directory [%s] ", outputFolder)
                        );
                    }
                }
            }

            writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(
                            String.format("%s/%s_query%s.out", outputFolder, outPrefix, queryId), true),
                    "utf-8"));

            writer.write(ouput);

        } catch (IOException e) {
            LOGGER.error(e.toString());
            throw new RuntimeException(e);
        } finally {
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (IOException e) {
                LOGGER.error(e.toString());
            }
        }

    }
}
