package com.gpjpe.helpers;


import org.apache.hadoop.hbase.util.Bytes;

public class Utils {
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
}
