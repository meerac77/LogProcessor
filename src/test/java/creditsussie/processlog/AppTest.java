package creditsussie.processlog;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class AppTest extends TestCase {

	public AppTest(String testName) {
		super(testName);
	}

	public static Test suite() {
		return new TestSuite(AppTest.class);
	}

	public void testApp() {
		String filePath = "C:\\Users\\Test\\logfile.txt";
		assertTrue(App.startProcessing(filePath));
	}
}
