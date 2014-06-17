package nl.utwente.dbpt.model;


/**
 * 
 * @author Anne
 *
 */
public class VisitorResult {

	public long time = 0;
	public int locks = 0;

	public VisitorResult(long time, int locks) {
		this.time = time;
		this.locks = locks;
	}

}
