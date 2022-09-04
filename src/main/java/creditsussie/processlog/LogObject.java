package creditsussie.processlog;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LogObject {

	@JsonProperty("id")
	String id;

	@JsonProperty("type")
	String type;

	@JsonProperty("host")
	String host;

	@JsonProperty("timestamp")
	long timestamp;

	@JsonProperty("state")
	String state;

	long startTime;
	long endTime;
	int duration;
	boolean alert;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	public int getDuration() {
		return duration;
	}

	public void setDuration(int duration) {
		this.duration = duration;
	}

	public boolean isAlert() {
		return alert;
	}

	public void setAlert(boolean alert) {
		this.alert = alert;
	}

	@Override
	public String toString() {
		return "LogObject [id=" + id + ", type=" + type + ", host=" + host + ", timestamp=" + timestamp + ", state="
				+ state + ", startTime=" + startTime + ", endTime=" + endTime + ", duration=" + duration + ", alert="
				+ alert + "]";
	}

}
