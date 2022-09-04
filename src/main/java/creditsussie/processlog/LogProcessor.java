package creditsussie.processlog;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class LogProcessor extends RecursiveTask<List<LogObject>> {

	private static final long serialVersionUID = 1L;

	private static final Logger logger = LoggerFactory.getLogger(LogProcessor.class);

	List<LogProcessor> processorTasks = new ArrayList<LogProcessor>();
	List<String> lines;
	List<LogObject> logList = new ArrayList<LogObject>();

	private static final int THRESHOLD = 1000;

	public LogProcessor(List<String> lines) {
		this.lines = lines;
	}

	@Override
	protected List<LogObject> compute() {
		if (lines.size() > THRESHOLD) {
			processorTasks.addAll(createSubTasks());
			for (LogProcessor task : processorTasks) {
				task.fork();
			}
			for (LogProcessor task : processorTasks) {
				logList.addAll(task.join());
			}
		} else {
			logList = processLogLines(lines);
		}
		return logList;
	}

	private List<LogProcessor> createSubTasks() {
		List<LogProcessor> subTasks = new ArrayList<LogProcessor>();
		int size = lines.size();
		for (int i = 0; size / THRESHOLD > 0; i += THRESHOLD) {
			LogProcessor task = new LogProcessor(lines.subList(i, i + THRESHOLD));
			subTasks.add(task);
			size -= THRESHOLD;
		}
		// logger.debug("Number of sub tasks returned = " + subTasks.size());
		return subTasks;
	}

	private List<LogObject> processLogLines(List<String> logLines) {

		List<LogObject> logList = new ArrayList<LogObject>();
		ObjectMapper mapper = new ObjectMapper();

		try {
			for (String line : logLines) {
				LogObject logObj = mapper.readValue(line, LogObject.class);
				logList.add(logObj);
			}
		} catch (JsonMappingException e) {
			logger.error("JsonMappingException: " + e.getStackTrace());
		} catch (JsonProcessingException e) {
			logger.error("JsonProcessingException: " + e.getStackTrace());
		}

		return logList;
	}

}
