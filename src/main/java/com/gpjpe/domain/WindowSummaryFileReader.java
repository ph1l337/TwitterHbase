package com.gpjpe.domain;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class WindowSummaryFileReader implements IWindowSummaryReader {

	String filePath;
	BufferedReader bufferedReader;
	boolean closed;

	public WindowSummaryFileReader(String filePath) {
		this.filePath = filePath;
		this.bufferedReader = null;
		this.closed = false;
	}

	public boolean isClosed() {
		return this.closed;
	}

	public WindowSummary next() {

		String line;

		try {
			if (this.bufferedReader == null) {
				this.bufferedReader = new BufferedReader(
						new InputStreamReader(
								new FileInputStream(
						this.filePath), Charset.forName("UTF-8").newDecoder()));

			}

			line = this.bufferedReader.readLine();

			if (line == null) {
				this.closed = true;
				this.bufferedReader.close();
				return null;
			}

			String[] tokens = line.split(",");
			WindowSummary windowSummary = new WindowSummary();
			Map<String, Integer> hashTagCount = new HashMap<String, Integer>();

			if (tokens.length % 2 != 0) {
				throw new RuntimeException(String.format(
						"Unexpected line contents: [%s]", line));
			}

			for (int i = 0; i < tokens.length; i++) {
				switch (i) {
				case 0:
					windowSummary.setWindow(Long.parseLong(tokens[i]));
					break;
				case 1:
					windowSummary.setLanguage(tokens[i].trim());
					break;
				default:
			
					int count = Integer.parseInt(tokens[i + 1]);
					
					if (count > 0) {
						hashTagCount.put(tokens[i].trim(), count);						
					}

					i++;
					break;
				}
			}

			windowSummary.setHashTagCountMap(hashTagCount);

			return windowSummary;
			
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
