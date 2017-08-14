package com.testSpark.case1;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class TestServer {

	public static void main(String[] args) throws Exception {
		ServerSocket server = new ServerSocket(9999);
		
		Socket socket = server.accept();
		
		System.out.println(socket.getInetAddress() + " is connected.");
		
		OutputStream os = socket.getOutputStream();
		
		PrintWriter pw = new PrintWriter(os);
		
		String line = null;
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		
		while ((line = reader.readLine()) != null) {
			pw.println(line);
			pw.flush();
		}
		
		pw.close();
		reader.close();
		socket.close();
		server.close();
	}

}
