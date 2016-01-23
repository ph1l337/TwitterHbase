package com.gpjpe.domain;

import java.util.Map;

public class WindowSummary {
	
	Long window;
	String language;
	Map<String, Long> hashTagCountMap;	
	
	public Long getWindow() {
		return window;
	}

	public void setWindow(Long window) {
		this.window = window;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public Map<String, Long> getHashTagCountMap() {
		return hashTagCountMap;
	}

	public void setHashTagCountMap(Map<String, Long> hashTagCounts) {
		this.hashTagCountMap = hashTagCounts;
	}
	
	public WindowSummary() {}
	
	public WindowSummary(Long window, String language,
			Map<String, Long> hashTagCounts) {
		super();
		this.window = window;
		this.language = language;
		this.hashTagCountMap = hashTagCounts;
	}
}
