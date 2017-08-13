package com.testSpark.case1;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;

public class TestClient {

	public static void main(String[] args) throws Exception {
		Socket socket = new Socket("localhost", 9999);
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		
		String line = null;
		
		while ((line = reader.readLine()) != null) {
			System.out.println(line);
		}
		
		reader.close();
		
		socket.close();
	}

}
