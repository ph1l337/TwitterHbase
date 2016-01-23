package com.gpjpe.domain;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class WindowSummaryReader {
	
	String filePath;
	BufferedReader bufferedReader;
	boolean closed;
	
	public WindowSummaryReader(String filePath) {
		this.filePath = filePath;
		this.bufferedReader = null;
		this.closed = false;
	}
	
	public boolean isClosed() {
		return this.closed;
	}
	
	public WindowSummary next() throws IOException {
		if (this.bufferedReader == null) {
			this.bufferedReader = new BufferedReader(new FileReader(this.filePath));
		}
		
		String line = this.bufferedReader.readLine();
		
		if (line == null) {
			this.closed = true;
			this.bufferedReader.close();
			return null;
		}
		
		String[] tokens = line.split(",");
		WindowSummary windowSummary = new WindowSummary();
		Map<String, Long> hashTagCount = new HashMap<String, Long>();
		
		if (tokens.length % 2 != 0) {
			throw new RuntimeException(String.format("Unexpected line contents: [%s]", line));
		}
		
		for(int i = 0; i < tokens.length; i++) {
			switch(i) {
			case 0:
				windowSummary.setWindow(Long.parseLong(tokens[i]));
				break;
			case 1:
				windowSummary.setLanguage(tokens[i]);
				break;
			default:
				hashTagCount.put(tokens[i], Long.parseLong(tokens[i+1]));
				i++;
				break;
			}
		}
		
		windowSummary.setHashTagCountMap(hashTagCount);
		
		return windowSummary;
	}
}
