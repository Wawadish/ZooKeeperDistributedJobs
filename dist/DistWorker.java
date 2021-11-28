import java.lang.Runnable;
import java.io.*;
import java.util.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.lang.Thread;
import java.lang.management.*;
import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;
import org.apache.zookeeper.data.*;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Op.GetData;
import org.apache.zookeeper.data.Stat;

public class DistWorker implements IDistComponent {

	private String workers_dir = "/dist34/workers";
	private String tasks_dir = "/dist34/tasks";

	private ZooKeeper aZk;
	String workerNodeName;
	String currentTaskName;
	

	public DistWorker(ZooKeeper pZk) throws KeeperException, InterruptedException{
		System.out.println("DISTAPP : Role : " + " I will be functioning as " + "worker");
		aZk = pZk;
		workerNodeName = aZk.create(workers_dir + "/worker-", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		getTask();
		System.out.println("DISTAPP : workerNodeName " + workerNodeName);

	}

	void getTask(){
		aZk.getData(workerNodeName, this, this, null);
	}

    public void process(WatchedEvent e) {
		if(e.getType() == Watcher.Event.EventType.NodeDataChanged){
			getTask();
		}
	}
	
	DistTask reconstructTask(byte[] taskSerial){
		// Re-construct our task object.
		DistTask dt = null;
		try{
			ByteArrayInputStream bis = new ByteArrayInputStream(taskSerial);
			ObjectInput in = new ObjectInputStream(bis);
			dt = (DistTask) in.readObject();
		} catch (IOException io) {
			System.out.println(io);
		} catch (ClassNotFoundException cne) {
			System.out.println(cne);
		}
		return dt;
	}

	void done(DistTask dt){
		String output_dir = tasks_dir + "/" + currentTaskName + "/result";
		try{
			// Serialize our Task object back to a byte array!
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(dt);
			oos.flush();
			byte[] taskSerial = bos.toByteArray();
			aZk.create(output_dir, taskSerial, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			System.out.println("DISTAPP : done : created " + output_dir);
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	void computeAsync(DistWorker worker, String currentTaskName){
		System.out.println("DISTAPP : computeAsync " + currentTaskName + " asynchronously");
		try{
			byte[] taskSerial = aZk.getData(tasks_dir + "/" + currentTaskName, false, null);
			DistTask task = reconstructTask(taskSerial);
			task.compute();
			System.out.println("DISTAPP : computeAsync" + currentTaskName + " is done, writing to /result");
			worker.done(task);
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	// Asynchronous callback that is invoked by the zk.getChildren request.
	public void processResult(int rc, String path, Object ctx, List<String> children) {
		return;
	}

	//getData callback
	@Override
	public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
		System.out.println("DISTAPP : processResult getData : " + rc + ":" + path + ":" + ctx);

		if(data == null){
			return;
		}

		currentTaskName = new String(data);
		System.out.println("DISTAPP : processResult starting work on task" + currentTaskName + " asynchronously");
		Thread thread = new Thread(() -> computeAsync(this, currentTaskName));
		thread.start();
	}

	// Asynchronous callback that is invoked by the aZk.exists and setData
	@Override
	public void processResult(int rc, String path, Object ctx, Stat stat) {
		return;
	}
}
