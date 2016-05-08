package edu.upenn.cis455.mapreduce.worker;

import java.io.File;
import java.io.IOException;

public class FileHelper {
	// Makes directory. Deletes any existing directory of the same name
	public static File makeDirDeleteIfExists(File file) {
		if (file.exists() && file.isDirectory()) {
			for (File f : file.listFiles()) {
				f.delete();
			}
			file.delete();
		}
		file.mkdirs();
		return file;
	}

	// Creates file. Deletes any existing file of the same name
	public static File touchDeleteIfExists(File file) {
		if (file.exists() && file.isFile()) {
			file.delete();
		} else {
			try {
				file.createNewFile();
			} catch (IOException e) {
			}
		}
		return file;
	}
	
	// Creates a file and any required directories. Deletes if exists
	public static void makeFile(File file) {
		if (file.exists()) {
			file.delete();
		}
		File dir = new File(file.getParentFile().getAbsolutePath());
		dir.mkdirs();
		try {
			file.createNewFile();
		} catch (IOException e) {
		}
	}
}
