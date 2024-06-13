package edu.utexas.cs.cs378;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.AbstractMap.*;
import java.util.stream.Collectors;

public class MainClass {

	public static void main(String[] args) throws IOException {
		// You should pass the file name and path as first argument of this main method.
		String file = "README.md";

		if (args.length > 0) {
			System.out.print("Running with file :"+ args[0]);		
			file = args[0];
		}

		Path path = Paths.get(file);

		Map<String, Long> wordCount = Files.lines(path)
				// Split the string by Space 
				.flatMap(line -> Arrays.stream(line.trim().split(" ")))
				// 
				.map(word -> word.replaceAll("[^a-zA-Z]", "").toLowerCase().trim()).filter(word -> word.length() > 0)
				// 
				.map(word -> new SimpleEntry<>(word, 1))
				// 
				.collect(Collectors.groupingBy(SimpleEntry::getKey, Collectors.counting()));

		wordCount.forEach((k, v) -> System.out.println(String.format("%s - %d", k, v)));

		
	}

}