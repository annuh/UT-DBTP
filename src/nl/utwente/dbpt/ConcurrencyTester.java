package nl.utwente.dbpt;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import jxl.Workbook;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;
import nl.utwente.dbpt.model.User;
import nl.utwente.dbpt.model.Visitor;
import nl.utwente.dbpt.model.VisitorResult;
import nl.utwente.dbpt.utils.ConnectionFactory;

public class ConcurrencyTester {

	private int visitors = 1000;
	private int pages = 10;
	private Connection conn = null;

	public static final String OPTIMISTIC = "OPTIMISTIC";
	public static final String PESSIMISTIC = "PESSIMISTIC";

	public ConcurrencyTester(){
		conn = ConnectionFactory.getConnection();
		try {
			fillDatabase(conn);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		ArrayList<Integer> visits1 = new ArrayList<Integer>();
		for(int i=0; i<100; i++){
			visits1.add(1);
		}
		
		Random generator = new Random(); 

		
		for(int r=1;r<200;r=r+5){
			for(int r2=0; r2<5; r2++) {
				ArrayList<Visitor> visitors = new ArrayList<Visitor>();
				int NUMBER_OF_VISITORS = 50;
				for(int i=1; i<=NUMBER_OF_VISITORS;i++){
					ArrayList<Integer> visits2 = new ArrayList<Integer>();
					
					
					for(int j=0; j<20; j++){
						int x = generator.nextInt(r) + 1;
						
						visits2.add(x);
					}
					visitors.add(new Visitor(visits2));
	
				}
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				//runConcurrencyModus(visitors, PESSIMISTIC);
				runConcurrencyModus(visitors, OPTIMISTIC);
			}
		}
		
	

		
		


	}

	public void runConcurrencyModus(ArrayList<Visitor> users, String modus) {
		//System.out.println("Start "+modus);
		try {
			conn.createStatement().executeUpdate("ALTER TABLE tickets SET "+modus);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		// http://www.vogella.com/tutorials/JavaConcurrency/article.html#futures

		ExecutorService executor = Executors.newCachedThreadPool();
		List<Future<VisitorResult>> list = new ArrayList<Future<VisitorResult>>();
		for (Visitor worker : users) {
			//Callable<VisitorResult> worker = new Visitor();
				
			Future<VisitorResult> submit = executor.submit(worker);
			list.add(submit);
		}
		long sum = 0;
		long time = 0;
		// now retrieve the result
		for (Future<VisitorResult> future : list) {
			try {
				sum += future.get().locks;
				time += future.get().time;
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
		System.out.print(""+sum);
		System.out.print(",");
		System.out.println(""+time);

		executor.shutdown();

	}

	private void fillDatabase(Connection connect) throws SQLException {
		Statement statement = connect.createStatement();

		statement.executeUpdate("DROP TABLE users");

		String sql = ""
				+ "CREATE TABLE users ("
				+ "id INT NOT NULL,"
				+ "username CHAR(127), "
				+ "C_NO INT DEFAULT 0,"
				+ "PRIMARY KEY (id)"
				+ ")";
		statement.executeUpdate(sql);
		
		statement.executeUpdate("DROP TABLE tickets");
		sql = ""
				+ "CREATE TABLE tickets ("
				+ "id INT NOT NULL,"
				+ "available INT DEFAULT 1, "
				+ "PRIMARY KEY (id)"
				+ ") STORE DISK";
		statement.executeUpdate(sql);
		
		statement.executeUpdate("ALTER TABLE users SET STORE DISK");
		statement.executeUpdate("ALTER TABLE tickets SET STORE DISK");

	//	statement.executeUpdate("SET LOCK TIMEOUT 10");

		PreparedStatement preparedStatement = connect.prepareStatement(
				"insert into users (id, username) values (?, ?)");
		
		PreparedStatement ticketStatement = connect.prepareStatement(
				"insert into tickets (id) values (?)");
		for(int i=1; i<=1000; i++){
			preparedStatement.setInt(1, i);
			preparedStatement.setString(2, "User "+i);
			preparedStatement.addBatch();
			
			ticketStatement.setInt(1, i);
			ticketStatement.addBatch();

		}
		preparedStatement.executeBatch();
		ticketStatement.executeBatch();

		//System.out.println("READY");
	}

	public static void main(String[] args) {

		new ConcurrencyTester();
	}


	/**
	 * Create list of pages that the users will visit. Do it here, so list will be same for both concurrency mechamisms.
	 * @return
	 */
	private ArrayList<Integer> getRandomVisits() {
		ArrayList<Integer> result = new ArrayList<Integer>();
		for(int i=1; i<visitors*pages; i++){

		}

		return result;
	}
}
