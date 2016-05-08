package edu.upenn.cis455.mapreduce.master;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;

public class BDBWrapper {

	// Required for BDB construction
	private EnvironmentConfig envConfig;
	private Environment env;
	private List<Database> databases;

	// Constructor
	public BDBWrapper(String directory) {

		if (directory == null) {
			System.err.println("BDB directory was null");
			System.exit(-1);
		}

		// Looks for file. Creates if it doesn't exist
		File dir = new File(directory);
		if (!dir.exists()) {
			dir.mkdirs();
			System.out.println("File Created");
		}

		// Initialize environment config to configurate the environment
		this.envConfig = new EnvironmentConfig();
		this.envConfig.setAllowCreate(true);
		this.env = new Environment(dir, this.envConfig);

		// Configure databases
		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setAllowCreate(true);

		// Create and setup the databases
		this.databases = new ArrayList<Database>();
		this.databases.add(this.env.openDatabase(null, "docID:pagerank", dbConfig));	// 1:1
		this.databases.add(this.env.openDatabase(null, "docID:title", dbConfig));		// 1:1
	}

	// Closes all databases and the BDB environment
	public void close() {
		for (Database db: this.databases) {
			db.close();
		}
		this.env.close();
	}

	// Puts a key value pair into a specified database. Will overwrite existing values
	public void putKeyValue(String dbName, String key, String value) {
		Database database = findDatabase(dbName);

		// return if the specified database was not found
		if (database == null) {
			return;
		}

		// Add the key value pair to the database
		DatabaseEntry dbKey = new DatabaseEntry(key.getBytes());
		DatabaseEntry dbValue = new DatabaseEntry(value.getBytes());
		database.put(null, dbKey, dbValue);
	}

	// Gets the value of a key from a specified database
	public String getKeyValue(String dbName, String key) {
		Database database = findDatabase(dbName);

		// return null if the specified database was not found
		if (database == null) {
			return null;
		}

		// Find the value of the given key
		DatabaseEntry dbKey = new DatabaseEntry(key.getBytes());
		DatabaseEntry dbValue = new DatabaseEntry();
		if (database.get(null, dbKey, dbValue, null) == OperationStatus.SUCCESS) {
			return new String(dbValue.getData());
		} else {
			return null;
		}
	}

	// Deletes a key value pair from the specified database. Also deletes lists
	public void deleteKeyValue(String dbName, String key) {
		Database database = findDatabase(dbName);

		// return if the specified database was not found
		if (database == null) {
			return;
		}

		// Delete the key value pair
		DatabaseEntry dbKey = new DatabaseEntry(key.getBytes());
		database.delete(null, dbKey);
	}

	// Puts a one to many key value pair into a specified database. For channel:xpaths
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void putKeyValueList(String dbName, String key, List<String> values) {
		Database database = findDatabase(dbName);

		// return if the specified database was not found
		if (database == null) {
			return;
		}

		// Put the key and the list of values into the database
		DatabaseEntry dbKey = new DatabaseEntry(key.getBytes());
		DatabaseEntry dbValues = new DatabaseEntry();
		EntryBinding<List<String>> binder = new SerialBinding(new StoredClassCatalog(database), List.class);
		binder.objectToEntry(values, dbValues);
		database.put(null, dbKey, dbValues);
	}

	// Gets the list values associated with a given key into a specified database. For channel:xpaths
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public List<String> getKeyValueList(String dbName, String key) {
		Database database = findDatabase(dbName);

		// return if the specified database was not found
		if (database == null) {
			return null;
		}

		// Put the key and the list of values into the database
		DatabaseEntry dbKey = new DatabaseEntry(key.getBytes());
		DatabaseEntry dbValues = new DatabaseEntry();
		EntryBinding<List<String>> binder = new SerialBinding(new StoredClassCatalog(database), List.class);
		if (database.get(null, dbKey, dbValues, null) == OperationStatus.SUCCESS) {
			return binder.entryToObject(dbValues);
		} else {
			return null;
		}
	}

	// Searches existing databases for a specified database
	private Database findDatabase(String dbName) {

		// Search through existing databases for the specified database
		for (Database db: this.databases) {
			if (db.getDatabaseName().equals(dbName)) {
				return db;
			}
		}
		return null;
	}
}
