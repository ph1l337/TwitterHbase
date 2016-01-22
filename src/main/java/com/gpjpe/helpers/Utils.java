package com.gpjpe.helpers;


import org.apache.hadoop.hbase.util.Bytes;

public class Utils {
    public enum Ending {high, low}
    public static byte[] generateKey(long timeStamp, String lang){
        byte[] key = new byte[10];
        System.arraycopy(Bytes.toBytes(timeStamp),0,key,0,8);
        System.arraycopy(Bytes.toBytes(lang),0,key,8,2);
        return key;
    }
    public static byte[] generateKey(long timeStamp, Ending ending){
        byte[] key = new byte[10];
        System.arraycopy(Bytes.toBytes(timeStamp),0,key,0,8);
        switch (ending) {
            case high:
                break;
        }
        return key;
    }
}
