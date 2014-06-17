package nl.utwente.dbpt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import nl.utwente.dbpt.model.Visitor;
import nl.utwente.dbpt.model.VisitorResult;
import nl.utwente.dbpt.utils.ConnectionFactory;

public class ConcurrencyTester {

	private Connection conn = null;

	public static final String OPTIMISTIC = "OPTIMISTIC";
	public static final String PESSIMISTIC = "PESSIMISTIC";
	
	public int NUMBER_OF_VISITORS = 2;
	public int TRANSACTIONS_PER_VISItOR = 10;

	public ConcurrencyTester(){
		conn = ConnectionFactory.getConnection();
		try {
			fillDatabase(conn);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Random generator = new Random(); 

		for(int r=1;r<100;r=r+1){
			for(int r2=0; r2<5; r2++) {
				ArrayList<Visitor> visitors = new ArrayList<Visitor>();
				
				for(int i=1; i<=NUMBER_OF_VISITORS;i++){
					ArrayList<Integer> visits2 = new ArrayList<Integer>();

					for(int j=0; j<TRANSACTIONS_PER_VISItOR; j++){
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

				runConcurrencyModus(visitors, PESSIMISTIC);
				//runConcurrencyModus(visitors, OPTIMISTIC);
			}
		}
	}

	public void runConcurrencyModus(ArrayList<Visitor> users, String modus) {
		try {
			conn.createStatement().executeUpdate("ALTER TABLE users SET "+modus);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}

		// http://www.vogella.com/tutorials/JavaConcurrency/article.html#futures
		ExecutorService executor = Executors.newCachedThreadPool();
		List<Future<VisitorResult>> list = new ArrayList<Future<VisitorResult>>();
		for (Visitor worker : users) {				
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
		
		sum = sum * (100/(NUMBER_OF_VISITORS*TRANSACTIONS_PER_VISItOR));
		
		// Output results
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
		
		// Needed to use all concurrency control mechanisms
		statement.executeUpdate("ALTER TABLE users SET STORE DISK");
		statement.executeUpdate("SET LOCK TIMEOUT 10MS");
		PreparedStatement preparedStatement = connect.prepareStatement(
				"insert into users (id, username) values (?, ?)");

		for(int i=1; i<=1000; i++){
			preparedStatement.setInt(1, i);
			preparedStatement.setString(2, "User "+i);
			preparedStatement.addBatch();

		}
		preparedStatement.executeBatch();
	}

	public static void main(String[] args) {
		new ConcurrencyTester();
	}

}
