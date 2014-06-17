package nl.utwente.dbpt;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import jxl.Workbook;
import jxl.write.Label;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;
import nl.utwente.dbpt.ConcurrencyTester;
import nl.utwente.dbpt.model.Visitor;
import nl.utwente.dbpt.model.VisitorResult;
import nl.utwente.dbpt.utils.ConnectionFactory;

public class ExcelWriter {

	private ArrayList<VisitorResult> optimisticResults = new ArrayList<VisitorResult>();
	private ArrayList<VisitorResult> pessimisticResults = new ArrayList<VisitorResult>();

	
	public ExcelWriter(){

	}
	
	public void addVisitorResult(VisitorResult result, String type) {
		if(type.equals(ConcurrencyTester.OPTIMISTIC)) {
			optimisticResults.add(result);
		} else if(type.equals(ConcurrencyTester.PESSIMISTIC)) {
			pessimisticResults.add(result);
		}

	}
	
	public void generate(){
		try {
			WritableWorkbook workbook = null;
			SimpleDateFormat sdfDate = new SimpleDateFormat("yyyyMMdd_HHmmss");//dd/MM/yyyy
			String strDate = sdfDate.format(new Date());
			workbook = Workbook.createWorkbook(new File(strDate+"_.xls"));
			
			WritableSheet sheet = null;
			sheet = workbook.createSheet("Results", 0);
			sheet.addCell(new Label(0, 0, "Conflicts (%)")); 
			
			
			workbook.write();
			workbook.close();
			
		} catch (Exception e){
			e.printStackTrace();
		}
	}

	
}
