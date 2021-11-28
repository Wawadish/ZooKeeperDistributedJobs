import java.io.*;

import java.util.*;
import java.lang.Thread;

// To get the name of the host.
import java.net.*;

//To get the process id.
import java.lang.management.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;
import org.apache.zookeeper.data.*;
import org.apache.zookeeper.KeeperException.Code;

// TODO
// Replace 34 with your group number.
// You may have to add other interfaces such as for threading, etc., as needed.
// This class will contain the logic for both your master process as well as the worker processes.
//  Make sure that the callbacks and watch do not conflict between your master's logic and worker's logic.
//		This is important as both the master and worker may need same kind of callbacks and could result
//			with the same callback functions.
//	For a simple implementation I have written all the code in a single class (including the callbacks).
//		You are free it break it apart into multiple classes, if that is your programming style or helps
//		you manage the code more modularly.
//	REMEMBER !! ZK client library is single thread - Watches & CallBacks should not be used for time consuming tasks.
//		Ideally, Watches & CallBacks should only be used to assign the "work" to a separate thread inside your program.
public class DistProcess implements Watcher
																		, AsyncCallback.ChildrenCallback
																		, AsyncCallback.DataCallback
{
	ZooKeeper zk;
	String zkServer, pinfo;
	boolean isMaster=false;
	HashMap<String, ActiveWorker> workerNodes = new HashMap<>();
	LinkedList<String> pendingTasks = new LinkedList<>();
	HashSet<String> seenTasks = new HashSet<>();

	//Used by the workers
	byte[] previousTask = null;
	String workerPath;

	DistProcess(String zkhost)
	{
		zkServer=zkhost;
		pinfo = ManagementFactory.getRuntimeMXBean().getName();
		System.out.println("DISTAPP : ZK Connection information : " + zkServer);
		System.out.println("DISTAPP : Process information : " + pinfo);
	}

	void startProcess() throws IOException, UnknownHostException, KeeperException, InterruptedException
	{
		zk = new ZooKeeper(zkServer, 1000, this); //connect to ZK.
		try
		{
			runForMaster();	// See if you can become the master (i.e, no other master exists)
			isMaster=true;
			getTasks(); // Install monitoring on any new tasks that will be created.
			// TODO monitor for worker tasks
			getWorkers();

		}catch(NodeExistsException nee)
		{ 
			// TODO: What else will you need if this was a worker process?
			isMaster=false;
			workerPath = zk.create("/dist34/workers/worker-", "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			zk.getData(workerPath, this, null, null); //Set watch to get notified when this worker's node's data is updated
		} 

		System.out.println("DISTAPP : Role : " + " I will be functioning as " +(isMaster?"master":"worker"));
	}

	// Master fetching task znodes...
	void getTasks()
	{
		zk.getChildren("/dist34/tasks", this, this, null);  
	}

	void getWorkers()
	{
		zk.getChildren("/dist34/workers", this, this, null);  
	}

	// Try to become the master.
	void runForMaster() throws UnknownHostException, KeeperException, InterruptedException
	{
		//Try to create an ephemeral node to be the master, put the hostname and pid of this process as the data.
		// This is an example of Synchronous API invocation as the function waits for the execution and no callback is involved..
		zk.create("/dist34/master", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}

	void checkPendingTasks()
	{
		if (!pendingTasks.isEmpty())
		{
			String path = pendingTasks.remove(0);
			zk.getData(path, null, this, null);
		}
	}

	void executeTask(byte[] taskSerial)
	{
		try 
		{
			// Re-construct our task object.
			ByteArrayInputStream bis = new ByteArrayInputStream(taskSerial);
			ObjectInput in = new ObjectInputStream(bis);
			DistTask dt = (DistTask) in.readObject();
			
			//Execute the task.
			dt.compute();
			
			// Serialize our Task object back to a byte array!
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(dt); oos.flush();
			taskSerial = bos.toByteArray();

			//Update the worker's node in the zk
			zk.setData(workerPath, taskSerial, -1, null, null);
			previousTask = taskSerial; //Save the previous task in case our watcher is triggered by the setData we just did.
			zk.getData(workerPath, this, null, null); //Reset the watcher to listen for the next task
		} 	
		catch(IOException io){System.out.println(io);}
		catch(ClassNotFoundException cne){System.out.println(cne);}
	}

	public void process(WatchedEvent e)
	{
		//Get watcher notifications.

		//!! IMPORTANT !!
		// Do not perform any time consuming/waiting steps here
		//	including in other functions called from here.
		// 	Your will be essentially holding up ZK client library 
		//	thread and you will not get other notifications.
		//	Instead include another thread in your program logic that
		//   does the time consuming "work" and notify that thread from here.

		System.out.println("DISTAPP : Event received : " + e);
		// Master should be notified if any new znodes are added to tasks.
		if(e.getType() == Watcher.Event.EventType.NodeChildrenChanged)
		{
			// There has been changes to the children of the node.
			// We are going to re-install the Watch as well as request for the list of the children.
			if (e.getPath().equals("/dist34/tasks")){
				getTasks();
			}

			else if (e.getPath().equals("/dist34/workers")){
				getWorkers();
			}
		}

		else if (e.getType() == Watcher.Event.EventType.NodeDataChanged && e.getPath().startsWith("/dist34/workers/")){
			zk.getData(e.getPath(), null, this, null);
		}
	}

	//Asynchronous callback that is invoked by the zk.getChildren request.
	public void processResult(int rc, String path, Object ctx, List<String> children)
	{

		//!! IMPORTANT !!
		// Do not perform any time consuming/waiting steps here
		//	including in other functions called from here.
		// 	Your will be essentially holding up ZK client library 
		//	thread and you will not get other notifications.
		//	Instead include another thread in your program logic that
		//   does the time consuming "work" and notify that thread from here.

		// This logic is for master !!
		//Every time a new task znode is created by the client, this will be invoked.

		// TODO: Filter out and go over only the newly created task znodes.
		//		Also have a mechanism to assign these tasks to a "Worker" process.
		//		The worker must invoke the "compute" function of the Task send by the client.
		//What to do if you do not have a free worker process?
		System.out.println("DISTAPP : processResult : " + rc + ":" + path + ":" + ctx);


		if (path.equals("/dist34/workers"))
		{
			for (String c : children)
			{
				System.out.println(c);
				//Add any new workers to the map
				if (!workerNodes.containsKey("/dist34/workers/"+c))
				{
					workerNodes.put("/dist34/workers/"+c, null); //A mapping to null signifies that the worker is idle.
					checkPendingTasks();
				}
			}
		}

		else if (path.equals("/dist34/tasks"))
		{
			for(String c: children)
			{
				System.out.println(c);
				String taskPath = path + "/" + c;
				if (!seenTasks.contains(taskPath)) {
					seenTasks.add(taskPath);
					zk.getData(taskPath, null, this, null);
				}
			}
		}
	}

	//Asynchronous callback that is invoked by the zk.getData request.
	public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)
	{
		if (path.startsWith("/dist34/tasks/"))
		{
			boolean allWorkersBusy = true;
			for (Map.Entry e : workerNodes.entrySet())
			{
				//This is an idle worker, so give it the task and mark it as busy
				if (e.getValue() == null)
				{
					e.setValue(new ActiveWorker(data, path));
					zk.setData((String) e.getKey(), data, -1, null, null);
					zk.getData((String) e.getKey(), this, null, null);

					allWorkersBusy = false;
					break;
				}
			}

			//No workers are currently available, so put task in waiting queue.
			if (allWorkersBusy)
			{
				pendingTasks.add(path);
			}
		}

		else if (path.startsWith("/dist34/workers/"))
		{
			if (isMaster)
			{
				//If the data is the same as the task we just passed to the worker, can happen if the watcher is activated before the worker node is updated.
				if (Arrays.equals(data, workerNodes.get(path).getData())){
					zk.getData(path, this, null, null); //Reset the watcher
				}

				else
				{
					AsyncCallback.StringCallback cb = null;
					String taskPath = workerNodes.get(path).getTaskPath();
					zk.create(taskPath + "/result", data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, cb, null);
					workerNodes.put(path, null);
					checkPendingTasks(); //Now that a worker node just went idle, check if there are any pending tasks.
				}
			}

			else
			{
				//If the data is the same as the task we just executed, can happen if the watcher is activated before the worker can update its own node.
				if (Arrays.equals(data, previousTask)){
					zk.getData(path, this, null, null); //Reset the watcher
				}

				else
				{
					new Thread(() -> {
						executeTask(data);
					}).start();
				}
			}
		}
	}

	public static void main(String args[]) throws Exception
	{
		//Create a new process
		//Read the ZooKeeper ensemble information from the environment variable.
		DistProcess dt = new DistProcess(System.getenv("ZKSERVER"));
		dt.startProcess();

		//Replace this with an approach that will make sure that the process is up and running forever.
		while (true)
		{
			Thread.sleep(10000);
		}
	}
}
