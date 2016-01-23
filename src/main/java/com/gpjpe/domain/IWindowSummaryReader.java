package com.gpjpe.domain;


public interface IWindowSummaryReader {
	
	public boolean isClosed();
	
	public WindowSummary next();
}
