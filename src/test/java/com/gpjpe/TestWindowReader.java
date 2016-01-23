package com.gpjpe;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import junit.framework.TestCase;

import com.gpjpe.domain.WindowSummary;
import com.gpjpe.domain.WindowSummaryFileReader;

public class TestWindowReader extends TestCase {
	
	private File file;
	private Object[][] summaries;
	
	@Override
	public void setUp() throws IOException {
		
		summaries = new Object[][]{
			new Object[] {1453500000L, "en", "A", 44, "B", 16, "C", 50},
			new Object[] {1453500000L, "en", "E", 323, "F", 161, "G", 20}
		};
		
		this.file = File.createTempFile(this.getClass().getName(), ".tmp");		
		BufferedWriter bfWriter = new BufferedWriter(new FileWriter(file));
		
		StringBuilder sb;
		
		for(int i = 0; i < summaries.length; i++) {
			
			sb = new StringBuilder();
			for(int j = 0; j < summaries[i].length; j++) {
				sb.append(summaries[i][j].toString());
				if (j < summaries[i].length - 1) {
					sb.append(",");
				}
			}
			sb.append("\n");
			
			bfWriter.write(sb.toString());
		}
		
		bfWriter.close();
	}
	
	@Override
	public void tearDown() {
		if (file.exists()) {
			assertTrue(file.delete());
		}
	}
	
	public void testNothing() throws IOException {
		
		BufferedReader bfReader = new BufferedReader(new FileReader(this.file));
		
		String line;
		while((line = bfReader.readLine()) != null){
			System.out.println(line);
		}
		
		bfReader.close();
	}
	
	public void testReadingWindowFile() throws IOException {
		
		WindowSummaryFileReader windowSummaryReader = new WindowSummaryFileReader(this.file.getAbsolutePath());
		WindowSummary summary;
		
		for(int i = 0; i < summaries.length; i++) {
			 summary = windowSummaryReader.next();
			 assertNotNull(summary);
			
			for(int j = 0; j < summaries[i].length; j++) {
				switch(j) {
					case 0:
						assertTrue(summary.getWindow().equals(summaries[i][j]));
						break;
					case 1:
						assertTrue(summary.getLanguage().equals(summaries[i][j]));
						break;
					default:
						assertTrue(summary.getHashTagCountMap().containsKey(summaries[i][j]));
						assertTrue(summary.getHashTagCountMap().get(summaries[i][j]).equals(summaries[i][j+1]));
						j++;
						break;
				}
			}
		}
		
		assertNull(windowSummaryReader.next());
		assertTrue(windowSummaryReader.isClosed());
	}
}
