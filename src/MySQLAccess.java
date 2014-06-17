

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import jxl.Workbook;
import jxl.write.Label;
import jxl.write.Number;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;
import jxl.write.WriteException;

public class MySQLAccess {
	private Connection connect1, connect2 = null;
	private PreparedStatement countStatement1, countStatement2 = null;
	
	/**
	 * Size of the table that is created
	 */
	private int user_count=100;

	/**
	 * Returns value of 'C_NO' field for a given ID
	 * @param statement
	 * @param id
	 * @return
	 * @throws SQLException
	 */
	private int findC_NO(PreparedStatement statement, int id) throws SQLException{
		int result = 0;
//		statement.setInt(1, id);
//		ResultSet res = statement.executeQuery();
//		res.first();
//		result = res.getInt(1);
		return result;
	}

	/**
	 * Start. Runs all updates for experiment
	 */
	public void readDataBase() {
		try {
			//Class.forName("com.mysql.jdbc.Driver");
	        java.sql.Driver d = (java.sql.Driver)Class.forName("solid.jdbc.SolidDriver").newInstance();


			String connectSQL = "jdbc:solid://localhost:2315/dba/dba";

			
			connect1 = DriverManager.getConnection(connectSQL);
			connect2 = DriverManager.getConnection(connectSQL);

			fillDatabase(connect1);

			String countSql = "SELECT C_NO FROM users WHERE id = ? LIMIT 1";
			countStatement1 = connect1.prepareStatement(countSql);
			countStatement2 = connect2.prepareStatement(countSql);

		} catch (Exception e) {
			e.printStackTrace();
		}
		WritableWorkbook workbook = null;
		WritableSheet[] sheets = new WritableSheet[4];
		
		String[] isolations = new String[1];
		isolations[0] = "REPEATABLE READ";
		//isolations[1] = "READ COMMITTED";
		//isolations[2] = "READ UNCOMMITTED";
		//isolations[3] = "SERIALIZABLE";
		isolations[0] = "SERIALIZABLE";
		
		try {
			SimpleDateFormat sdfDate = new SimpleDateFormat("yyyyMMdd_HHmmss");//dd/MM/yyyy
			Date now = new Date();
			String strDate = sdfDate.format(now);
			workbook = Workbook.createWorkbook(new File(strDate+"_"+user_count+".xls"));
			
			for(int i=0; i<isolations.length; i++) {
				sheets[i] = workbook.createSheet(isolations[i], 0);
				sheets[i].addCell(new Label(0, 0, "Conflicts (%)")); 
				sheets[i].addCell(new Label(1, 0, "Optimistic locking")); 
				sheets[i].addCell(new Label(2, 0, "Pessimistic locking"));
			}
		} catch (IOException | WriteException e) {
			e.printStackTrace();
		}

		int i=1;
		try {
			for(int isolation=0; isolation<isolations.length; isolation++) {
				String sql = "SET TRANSACTION ISOLATION LEVEL "+isolations[isolation];
  
				//connect1.createStatement().executeUpdate(sql);
				//connect2.createStatement().executeUpdate(sql);
				for(int conflicts=0; conflicts<=100; conflicts+=10){
				


					sheets[isolation].addCell(new Number(0, i, conflicts));
					
					String conflict_list = this.getRandomIDs(conflicts);
					System.out.println("Conflicts:" + conflict_list);
					long time = 0;
					int iters = 1;
					for(int iter=1; iter<=iters; iter++){
						connect1.setAutoCommit(true);
						connect2.setAutoCommit(true);
						connect1.createStatement().executeUpdate("ALTER TABLE users SET OPTIMISTIC");
						time += this.testLocking(conflict_list);
					}
					sheets[isolation].addCell(new Number(1, i, time));

					
					time = 0;
					for(int iter=1; iter<=iters; iter++){
						connect1.setAutoCommit(true);
						connect2.setAutoCommit(true);
						connect1.createStatement().executeUpdate("ALTER TABLE users SET PESSIMISTIC");
						time += this.testLocking(conflict_list);
					}
					sheets[isolation].addCell(new Number(2, i, time));
	
					i++;
					System.out.println("Ready with "+conflicts);
				}
			}

			workbook.write();
			workbook.close();
		} catch (WriteException | IOException | SQLException e1) {
			e1.printStackTrace();
		}
	}
	
	private long testLocking(String list) throws SQLException {
		long startTime = System.currentTimeMillis();
		connect1.setAutoCommit(false);
		connect2.setAutoCommit(false);

				
		String sql = "UPDATE users "
				+ "SET C_NO = C_NO + 1 "
				+ "WHERE id = ? ";

		
		PreparedStatement incrementStatement1 = connect1.prepareStatement(sql);
		PreparedStatement incrementStatement2 = connect2.prepareStatement(sql);

		for(int i=1; i<=user_count; i++){
			incrementStatement1.setInt(1, i);
			incrementStatement1.executeUpdate();

			if(list.contains(String.valueOf(i))) {
				incrementStatement2.setInt(1, i);
				try {
					incrementStatement2.executeUpdate(); // Should fail
					connect2.commit();
				} catch(SQLException e) {
					System.out.println("LOCK");
					connect2.rollback();
				} 
			}
			connect1.commit();

			
		}

		
		long endTime = System.currentTimeMillis();

		
		return (endTime - startTime); // Subtract 1s for every conflict
	}

	private long testPessimisticLocking(String list) throws SQLException {
		long startTime = System.currentTimeMillis();
		connect1.createStatement().executeUpdate("ALTER TABLE users SET PESSIMISTIC");
		
		connect1.setAutoCommit(false);
		connect2.setAutoCommit(false);
		connect1.createStatement().executeQuery(""
				+ "SELECT * "
				+ "FROM users "
				+ "WHERE id IN ("+list+") ");
		// Now connection1 blocks the rows in list
		
		long correction = 0;

		
		PreparedStatement incrementStatement2 = connect2.prepareStatement(""
				+ "UPDATE users "
				+ "SET C_NO = C_NO + 1 "
				+ "WHERE id = ?");
		for(int i=1; i<=user_count; i++){
			// Following executeUpdate() will fail for locked rows
			try {
				incrementStatement2.setInt(1, i);
				incrementStatement2.execute();
			} catch (SQLException e) {
				// Lock! Continue...
			}
		}
		long endTime = System.currentTimeMillis();

		connect1.commit(); // Release lock
		
		return (endTime - startTime) - correction; // Subtract 1s for every conflict
	}

	private long testOptimisticLocking(String stringlist) throws SQLException {
		List<String> list = new ArrayList<String>(Arrays.asList(stringlist.split(",")));
		connect1.createStatement().executeUpdate("ALTER TABLE users SET OPTIMISTIC");

		long startTime = System.currentTimeMillis();
		long correction = 0;
		
		String sql = "UPDATE users "
				+ "SET C_NO = C_NO + 1 "
				+ "WHERE id = ? ";

		
		PreparedStatement incrementStatement1 = connect1.prepareStatement(sql);
		PreparedStatement incrementStatement2 = connect2.prepareStatement(sql);

		for(int i=1; i<=user_count; i++){
			//int C_NO1 = findC_NO(countStatement1, i);
			connect1.setAutoCommit(false);

			incrementStatement1.setInt(1, i);

			int rows = incrementStatement1.executeUpdate(); //Should fail in case of conflict

			if(list.contains(String.valueOf(i))) {
				long correction_start = System.currentTimeMillis();
				
				//int C_NO2 = findC_NO(countStatement2, i);

				incrementStatement2.setInt(1, i);
				//incrementStatement2.setInt(2, C_NO2);
				//correction += (System.currentTimeMillis() - correction_start);
				try {
					incrementStatement2.executeUpdate(); // Should succeed

				} catch(SQLException e) {
					//System.out.println("LOCK");
					//correction++;
					connect2.rollback();
					//connect1.rollback();
				} 
			}
			connect1.commit();

			//incrementStatement1.setInt(2, C_NO1);
			
		}

		//System.out.println(""+correction);
		long endTime = System.currentTimeMillis();
		return (endTime - startTime) - correction;
	}

	private String getRandomIDs(int conflicts) {
		conflicts = conflicts * user_count / 100; // Since conflicts is a percentage
		ArrayList<Integer> numbers = new ArrayList<Integer>();
		StringBuilder result = new StringBuilder();
		for(int i = 1; i <= user_count; i++) {
			numbers.add(i);
		}

		Collections.shuffle(numbers);
		for(int j =0; j < conflicts; j++){
			result.append(numbers.get(j) + ",");
		}

		if ( result.length() > 0 ) {
			result.deleteCharAt( result.length() - 1 );
			return result.toString();
		} else {
			return "-1"; // No records will be locked
		}

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
		statement.executeUpdate("ALTER TABLE users SET STORE DISK");
		statement.executeUpdate("SET LOCK TIMEOUT 10MS");

		PreparedStatement preparedStatement = connect.prepareStatement(
				"insert into users (id, username) values (?, ?)");
		for(int i=1; i<=user_count; i++){
			preparedStatement.setInt(1, i);
			preparedStatement.setString(2, "User "+i);
			preparedStatement.addBatch();
		}
		preparedStatement.executeBatch();
		System.out.println("READY");
	}

} 