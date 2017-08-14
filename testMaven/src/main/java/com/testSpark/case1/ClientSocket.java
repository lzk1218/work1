package com.testSpark.case1;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;

public class ClientSocket {

	public static void main(String[] args) throws Exception {
		
		Socket socket = new Socket("10.101.68.49", 9999);
		
		
		OutputStream os = socket.getOutputStream();
		PrintWriter pw = new PrintWriter(os);
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		
		String line = null;
		while ((line = reader.readLine()) != null) {
			pw.println(line);
		}
		
		pw.close();
		reader.close();
	}

}
