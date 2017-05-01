package simpledb.testing;
import simpledb.server.SimpleDB;

public class TestDriver {
	public static void main(String args[]) throws Exception {
		// initialise and test the new database
		SimpleDB.init(args[0]);
		TestBufferManagement.testBufferReplacementPolicy();
	}	
}