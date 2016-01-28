package com.gpjpe.helpers;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;

public class Utils {
	private static Logger LOGGER = LoggerFactory.getLogger(Utils.class
			.getName());

	public enum QueryKey {
		start, end
	}

	public static byte[] generateKey(long timeStamp, String lang) {
		byte[] key = new byte[13];

		byte[] tsPart = Bytes.toBytes(timeStamp);
		byte[] langPart = new byte[5];

		//fill langPart with 0s
		for (byte b : langPart){
			langPart[b] = (byte) -255;
		}
		//fill up with chars
		for(int i = 0; i < lang.toCharArray().length;i++){
			langPart[i] = (byte) lang.toCharArray()[i];
		}

		System.arraycopy(tsPart, 0, key, 0, 8);
		System.arraycopy(langPart, 0, key, 8, 5);
		return key;
	}

	public static byte[] generateKey(long timeStamp, QueryKey queryKey) {
		byte[] key = new byte[13];
		System.arraycopy(Bytes.toBytes(timeStamp), 0, key, 0, 8);
		switch (queryKey) {
		case start:
			byte[] low = { (byte) -255, (byte) -255,(byte) -255, (byte) -255,(byte) -255 };
			System.arraycopy(low, 0, key, 8, 5);
			break;
		case end:
			byte[] high = { (byte) 255, (byte) 255,(byte) 255, (byte) 255,(byte) 255 };
			System.arraycopy(high, 0, key, 8, 5);
			break;
		}
		return key;
	}

	public static void writeToFile(String output, String outputFolder,
			int queryId, String outPrefix) {
		BufferedWriter writer = null;
		try {

			File dir = new File(outputFolder);
			if (!dir.exists()) {
				if (!dir.mkdirs()) {
					if (!dir.exists()) {
						throw new RuntimeException(
								String.format(
										"Couldn't create directory [%s] ",
										outputFolder));
					}
				}
			}

			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(String.format("%s/%s_query%s.out",
							outputFolder, outPrefix, queryId), true), Charset
							.forName("UTF-8").newEncoder()));

			writer.write(output);

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
