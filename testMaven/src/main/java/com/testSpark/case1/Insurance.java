package com.testSpark.case1;

import java.io.Serializable;

public class Insurance implements Serializable{
	
	private static final long serialVersionUID = 4656424858603538171L;
	
	private String insurId;
	private String userId;
	public String getInsurId() {
		return insurId;
	}
	public void setInsurId(String insurId) {
		this.insurId = insurId;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	
	
}
