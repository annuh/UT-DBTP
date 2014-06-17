package nl.utwente.dbpt.model;

import java.sql.Connection;
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
public class Clerk implements Callable<VisitorResult> {

	private ArrayList<Integer> pagesToVisit = null;
	private int locks = 0;

	Connection conn = null;

	public Clerk(ArrayList<Integer> pagesToVisit) {
		this.pagesToVisit = pagesToVisit;

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
		
		visitPages();
		
		//System.out.println("Done visiting pages. Locks: "+locks);
		
		long totalTime = System.currentTimeMillis() - startTime;
		
		return new VisitorResult(totalTime, locks);
	}

	private void visitPages() {
		while(pagesToVisit.size() > 0) {
			
			ArrayList<Integer> doneList = new ArrayList<Integer>();
			for(int i=0; i<pagesToVisit.size(); i++){
			//for(Integer page : pagesToVisit){
				Integer page = pagesToVisit.get(i);
				try{
					// TODO: Move it to outside for loop, to improve performance
					String sql = "UPDATE users SET C_NO = C_NO + 1 WHERE id = "+ page;
					Statement statement = conn.createStatement();
					System.out.println(""+statement.executeUpdate(sql));
					
					statement.close();
					doneList.add(page);
					
				} catch(SQLException e) {
					//e.printStackTrace();
					//java.sql.SQLException: [Solid JDBC 6.5.0.0 Build 0010] SOLID Database Error 10006: Concurrency conflict, two transactions updated or deleted the same row
					locks++;
					//System.out.println("Lock for page "+ page);
				}
			}
			
			pagesToVisit.removeAll(doneList);
		}
		try {
			conn.commit();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
