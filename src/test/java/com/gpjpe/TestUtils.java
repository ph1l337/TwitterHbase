package com.gpjpe;
import com.gpjpe.helpers.Utils;
import junit.framework.TestCase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.file.tfile.ByteArray;

import java.util.Arrays;


public class TestUtils extends TestCase{

    public void testKeyGen(){
        long timestamp = 1453191726000L;
        String lang = "mkd";
        byte[] tsPart = new byte[8];
        byte[] langPart = new byte[5];


        System.out.println(Arrays.toString(Utils.generateKey(timestamp, lang)));


        System.arraycopy(Utils.generateKey(timestamp, lang),0, tsPart,0,8);
        System.arraycopy(Utils.generateKey(timestamp, lang),8, langPart,0,5);
        System.out.println(Bytes.toLong(tsPart) + Bytes.toString(langPart));
        assertEquals(Utils.generateKey(timestamp,lang).length,13);
        //assertEquals(Bytes.toString(langPart),lang);

    }
    public void testKeyGenWithoutLangLow(){
        long timestamp = 1453191726000L;
        byte[] tsPart = new byte[8];
        byte[] langPart = new byte[5];

        System.out.println(Arrays.toString(Utils.generateKey(timestamp, Utils.QueryKey.start)));


        System.arraycopy(Utils.generateKey(timestamp, Utils.QueryKey.start),0, tsPart,0,8);
        System.arraycopy(Utils.generateKey(timestamp, Utils.QueryKey.start),8, langPart,0,5);
        System.out.println(Bytes.toLong(tsPart) + Bytes.toString(langPart));
        assertEquals(Utils.generateKey(timestamp,Utils.QueryKey.start).length,13);
        //assertEquals(Bytes.toString(langPart),lang);

    }
    public void testKeyGenWithoutLangHigh(){
        long timestamp = 1453191726000L;
        byte[] tsPart = new byte[8];
        byte[] langPart = new byte[5];

        System.out.println(Arrays.toString(Utils.generateKey(timestamp, Utils.QueryKey.end)));

        System.arraycopy(Utils.generateKey(timestamp, Utils.QueryKey.end),0, tsPart,0,8);
        System.arraycopy(Utils.generateKey(timestamp, Utils.QueryKey.end),8, langPart,0,5);
        System.out.println(Bytes.toLong(tsPart) + Bytes.toString(langPart));
        assertEquals(Utils.generateKey(timestamp,Utils.QueryKey.end).length,13);
        //assertEquals(Bytes.toString(langPart),lang);

    }
}

