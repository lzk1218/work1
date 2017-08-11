package com.testSpark.case1;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

public class MyReceiver extends Receiver<String> {
	
	private static final long serialVersionUID = 1L;
	private String path;
	
	public MyReceiver(String path) {
		super(StorageLevel.MEMORY_AND_DISK_2());
		this.path = path;
	}
	
	@Override
	public void onStart() {
		// TODO Auto-generated method stub
		new Thread(this::receive).start();
	}

	@Override
	public void onStop() {
		// TODO Auto-generated method stub
		
	}
	
	private void receive() {
		try {
			List<String> list = Files.readAllLines(Paths.get(path));
			list.forEach(line -> store(line));
			
			restart("Trying to run again");
		} catch (IOException e) {
			e.printStackTrace();
			restart("Trying to run again");
		}
		
	}
	
}
