import java.lang.Runnable;
import java.io.*;
import java.util.*;

import javax.sound.sampled.SourceDataLine;

import java.net.*;
import java.lang.Thread;
import java.lang.management.*;
import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;
import org.apache.zookeeper.data.*;
import org.apache.zookeeper.proto.SetWatches;
import org.apache.zookeeper.server.watch.WatcherMode;
import org.apache.zookeeper.KeeperException.Code;

public class DistMaster implements IDistComponent{

	private String workers_dir = "/dist34/workers";
	private String tasks_dir = "/dist34/tasks";

	private ZooKeeper aZk;
	private LinkedHashSet<String> availableWorkers;
	private HashSet<String> workerView;

	private LinkedList<String> taskQueue;
	private HashMap<String, String> assignedTasks;


    public DistMaster(ZooKeeper pZk){
		System.out.println("DISTAPP : Role : " + " I will be functioning as " + "master");
		aZk = pZk;

		availableWorkers = new  LinkedHashSet<String>();
		workerView = new HashSet<String>();
		
		taskQueue = new LinkedList<String>();
		assignedTasks = new HashMap<String, String>();

		getWorkers();
		getTasks();
    }

    // Set watcher on tasks & async call to getChildren
	void getTasks() {
		aZk.getChildren(tasks_dir, this, this, null);
	}

	// Set watcher on workers & async call to getChildren
	void getWorkers(){
		aZk.getChildren(workers_dir, this, this, null);
	}

	// Watcher triggered
	@Override
    public void process(WatchedEvent e) {

		System.out.println("DISTAPP : process: event received : " + e);

		if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged){

			if(e.getPath().equals(tasks_dir)){
				System.out.println("DISTAPP : task creation/deletion detected.");
				getTasks();

			}else if(e.getPath().equals(workers_dir)){
				System.out.println("DISTAPP : worker creation/deletion detected.");
				getWorkers();
			}
		}

		if(e.getType() == Watcher.Event.EventType.NodeCreated){
			aZk.exists(e.getPath(), false, this, null);
		}

	}

    void processTasksChange(List<String> tasks){
		System.out.println("DISTAPP : processTasksChange");
		for (String taskId : tasks) {
			if(!assignedTasks.containsKey(taskId)){
				System.out.println("DISTAPP : processTasksChange : Adding new task " + taskId + " to queue.");
				assignedTasks.put(taskId, null);
				taskQueue.add(taskId);
			}
		}
		dispatchTasksAsync();
	}

	void processWorkersChange(List<String> workers){
		System.out.println("DISTAPP : processWorkersChange");

		// Updating view
		workerView.retainAll(new HashSet(workers));
		// Removing removed workers from available workers
		availableWorkers.retainAll(workerView);

		for(String worker : workers){
			if(!workerView.contains(worker)){
				System.out.println("DISTAPP : processWorkersChange : Adding new worker " + worker + " to the view, worker is available.");
				workerView.add(worker);
				availableWorkers.add(worker);
			}
		}
		dispatchTasksAsync();
	}

	void dispatchTasksAsync(){
		System.out.println("DISTAPP : dispatchTasksAsync task queue " + String.valueOf(taskQueue.size()) + 
			" available workers " + String.valueOf(availableWorkers.size()));

		while(!taskQueue.isEmpty() && !availableWorkers.isEmpty()){

			String taskId = taskQueue.pop();
			String workerId = availableWorkers.iterator().next();
			availableWorkers.remove(workerId);
			
			System.out.println("DISTAPP : dispatchTasksAsync : assigning task " + taskId + " to worker " + workerId);
			assignedTasks.put(taskId, workerId);

			byte[] taskIdSerial = taskId.getBytes();

			try{

				// Watch for a result
				System.out.println("DISTAPP : dispatchTasksAsync : setting watcher on " + tasks_dir + "/" + taskId + "/result");
				aZk.exists(tasks_dir + "/" + taskId + "/result", this, this, null);

				// Give task to worker
				System.out.println("DISTAPP : dispatchTasksAsync : writing serial task to worker " + workerId);
				aZk.setData(workers_dir +  "/" + workerId, taskIdSerial, -1);

			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
	
	// Asynchronous callback that is invoked by the aZk.getChildren request.
	@Override
	public void processResult(int rc, String path, Object ctx, List<String> children) {
		System.out.println("DISTAPP : processResult getChildren : " + rc + ":" + path + ":" + ctx);
		if(path.equals(tasks_dir)){
			processTasksChange(children);
		} else if(path.equals(workers_dir)){
			processWorkersChange(children);
		}
	}

	// Asynchronous callback that is invoked by the aZk.getData request.
	@Override
	public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
		return;
	}
	
	// Asynchronous callback that is invoked by the aZk.exists and setData
	@Override
	public void processResult(int rc, String path, Object ctx, Stat stat) {
		if(stat == null){
			return;
		}
		System.out.println("DISTAPP : processResult exists : " + rc + ":" + path + ":" + ctx);
		String[] names = path.split("/");

		// dist34 - tasks - taskName - result
		String taskName = names[3];
		System.out.println("DISTAPP : processResult exists : taskName retrieved from path " + taskName);
		String workerName = assignedTasks.get(taskName);
		availableWorkers.add(workerName);
		dispatchTasksAsync();
	}
}
