package com.testMaven.testJava;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class Case1 {

	public static void main(String[] args) throws Exception{
		List<String> lines = Files.readAllLines(Paths.get("D:\\spark-test\\case1\\user.csv"));
		lines.forEach(line-> System.out.println(line));
	}

}
