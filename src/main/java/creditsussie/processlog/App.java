package creditsussie.processlog;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
	public static void main(String[] args) throws InterruptedException {

		final Logger logger = LoggerFactory.getLogger(App.class);

		logger.info("Processing Starts...");

		if (args.length < 1) {
			logger.error("Please include the path of the log file as command line argument");
			System.exit(1);
		}

		String pathName = args[0];

		pathName = pathName.trim();
		if (null == pathName || pathName.length() == 0) {
			logger.error("Please enter a valid value for the path of the log file");
			System.exit(1);
		}

		File file = new File(pathName);
		long fileLength = file.length();

		if (fileLength == 0) {
			logger.error("Please use a valid log file");
			System.exit(1);
		}

		BufferedReader bReader = null;
		try {
			List<String> lines = new ArrayList<String>();
			bReader = new BufferedReader(new FileReader(file));
			logger.info("File reading starts...");
			String line = bReader.readLine();
			while (null != line) {
				lines.add(line);
				line = bReader.readLine();
			}
			logger.info("File reading ends!");
			logger.debug("Number of log lines read from file = " + lines.size());
			
			LogProcessor lProcessor = new LogProcessor(lines);
			ForkJoinPool lThreadPool = new ForkJoinPool();
			lThreadPool.execute(lProcessor);
			lThreadPool.shutdown();
			List<LogObject> logList = lProcessor.join();
			logger.debug("Number of log lines processed = " + logList.size());
			
			LogObjectProcessor lObjProcessor = new LogObjectProcessor(logList);
			ForkJoinPool pThreadPool = new ForkJoinPool();
			pThreadPool.execute(lObjProcessor);
			pThreadPool.shutdown();
			List<ProcessedLogObject> results = lObjProcessor.join();
			logger.debug("Number of records to be inserted = " + results.size());
			
			DBOperations db = new DBOperations();
			db.writeToDB(results);
			
		} catch (FileNotFoundException e) {
			logger.error("FileNotFoundException: " + e.getStackTrace()); 
		} catch (IOException e) {
			logger.error("IOException: " + e.getStackTrace());
		} finally {
			if (null != bReader) {
				try {
					bReader.close();
				} catch (IOException e) {
					logger.error("IOException: " + e.getStackTrace());
				}
			}
		}
		
		//ProcessFile processor = new ProcessFile();
		//processor.processLogFile(file);
		//List<LogObject> logObjList = processor.processLogFile(file);
		//System.out.println("Size = " + logObjList.size());

		logger.info("Processing Ends!");

	}
}
