package creditsussie.processlog;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveTask;

public class LogObjectProcessor extends RecursiveTask<List<ProcessedLogObject>> {

	private static final long serialVersionUID = 1L;

	List<LogObjectProcessor> processorTasks = new ArrayList<LogObjectProcessor>();
	List<LogObject> logList;
	List<ProcessedLogObject> procList = new ArrayList<ProcessedLogObject>();
	List<String> idList = new ArrayList<String>();

	private static final int THRESHOLD = 1000;

	public LogObjectProcessor(List<LogObject> lines) {
		this.logList = lines;
	}

	@Override
	protected List<ProcessedLogObject> compute() {
		if (logList.size() > THRESHOLD) {
			processorTasks.addAll(createSubTasks());
			for (LogObjectProcessor task : processorTasks) {
				task.fork();
			}
			for (LogObjectProcessor task : processorTasks) {
				procList.addAll(task.join());
			}
		} else {
			procList = processLogLines(logList);
		}
		return procList;
	}

	private List<ProcessedLogObject> processLogLines(List<LogObject> list) {
		for (LogObject obj : list) {
			String id = obj.getId();
			if (!idList.contains(id)) {
				idList.add(id);
				ProcessedLogObject procObj = createProccessedObj(obj);
				procList.add(procObj);
			} else {
				modifyProccessedObj(obj);
			}

		}
		return procList;
	}

	private List<LogObjectProcessor> createSubTasks() {
		List<LogObjectProcessor> subTasks = new ArrayList<LogObjectProcessor>();
		int size = logList.size();
		for (int i = 0; size / THRESHOLD > 0; i += THRESHOLD) {
			LogObjectProcessor task = new LogObjectProcessor(logList.subList(i, i + THRESHOLD));
			subTasks.add(task);
			size -= THRESHOLD;
		}
		return subTasks;
	}

	private ProcessedLogObject createProccessedObj(LogObject obj) {
		ProcessedLogObject processedObj = new ProcessedLogObject();

		processedObj.setId(obj.getId());

		if (null != obj.getType())
			processedObj.setType(obj.getType());

		if (null != obj.getHost())
			processedObj.setHost(obj.getHost());

		if ("STARTED".equalsIgnoreCase(obj.getState()))
			processedObj.setStartTime(obj.getTimestamp());
		else if ("FINISHED".equalsIgnoreCase(obj.getState()))
			processedObj.setEndTime(obj.getTimestamp());

		return processedObj;
	}

	private void modifyProccessedObj(LogObject obj) {
		for (ProcessedLogObject procObj : procList) {
			if (obj.getId().equalsIgnoreCase(procObj.getId())) {
				if ("STARTED".equalsIgnoreCase(obj.getState()))
					procObj.setStartTime(obj.getTimestamp());
				else if ("FINISHED".equalsIgnoreCase(obj.getState()))
					procObj.setEndTime(obj.getTimestamp());

				int duration = (int) (procObj.getEndTime() - procObj.getStartTime());
				procObj.setDuration(duration);

				if (duration > 4)
					procObj.setAlert(true);
			}
		}
	}

}
