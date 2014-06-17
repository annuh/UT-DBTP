package nl.utwente.dbpt.model;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.concurrent.Callable;

import nl.utwente.dbpt.utils.ConnectionFactory;

/**
 * 
 * @author Anne
 *
 */
public class User implements Callable<VisitorResult> {

	private ArrayList<Integer> tickets = null;
	private int locks = 0;

	Connection conn = null;

	public User(ArrayList<Integer> tickets) {
		this.tickets = tickets;

		conn = ConnectionFactory.getConnection();
		try {
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public VisitorResult call() throws Exception {

		long startTime = System.currentTimeMillis();
		
		buyTickets();
		//transfer();
		//System.out.println("Done visiting pages. Locks: "+locks);
		
		long totalTime = System.currentTimeMillis() - startTime;
		
		return new VisitorResult(totalTime, locks);
	}
	
	public void transfer() {
		locks=0;
		try{
			for(Integer ticket : tickets) {
				String sql = "UPDATE tickets SET available = available + 1 WHERE id = "+ticket;

				Statement statement = conn.createStatement();
				statement.executeUpdate(sql);
				statement.close();
				
				 sql = "UPDATE tickets SET available = available + 1 WHERE id = "+ticket+1;

				 statement = conn.createStatement();
				//statement.executeUpdate(sql);
				statement.close();

			}
			
		} catch(SQLException  e) {
			//e.printStackTrace();
			//java.sql.SQLException: [Solid JDBC 6.5.0.0 Build 0010] SOLID Database Error 10006: Concurrency conflict, two transactions updated or deleted the same row
			//[Solid JDBC 6.5.0.0 Build 0010] SOLID Table Error 13031: Data dictionary operation is active for accessed table or index
			locks++;
		}
		
		try {
			conn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}		
	}

	private void buyTickets() {
		int amount = 4;
		locks = 0;
		try{
			for(Integer ticket : tickets) {
				String sql = "SELECT available FROM tickets WHERE id = "+ticket+ "";

				Statement statement = conn.createStatement();
				statement.executeQuery(sql);
				statement.close();

			}
			Thread.sleep(1000);
			for(Integer ticket : tickets) {
				
				String sql = "UPDATE tickets SET available = 0 WHERE id = "+ticket;
				Statement statement = conn.createStatement();
				int rows = statement.executeUpdate(sql);
				//System.out.println(""+rows);
				
				statement.close();
				conn.commit();
			}
		} catch(SQLException | InterruptedException  e) {
			e.printStackTrace();
			//java.sql.SQLException: [Solid JDBC 6.5.0.0 Build 0010] SOLID Database Error 10006: Concurrency conflict, two transactions updated or deleted the same row
			//[Solid JDBC 6.5.0.0 Build 0010] SOLID Table Error 13031: Data dictionary operation is active for accessed table or index
			locks++;
		}
		
//		try {
//			conn.commit();
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//		//	e.printStackTrace();
//		}
	}
	
	private String formatArrayListasList(ArrayList<Integer> items) {
		StringBuilder sb = new StringBuilder();
		
		for(Integer item : items) {
			sb.append(item);
			sb.append(",");
		}
		
		if ( sb.length() > 0 ) {
			sb.deleteCharAt( sb.length() - 1 );
			return sb.toString();
		} else {
			return "-1"; // No records will be locked
		}
	}
	
}
