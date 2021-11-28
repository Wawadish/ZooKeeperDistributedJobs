import java.io.*;

import java.util.*;

// To get the name of the host.
import java.net.*;
import java.lang.Thread;

//To get the process id.
import java.lang.management.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;
import org.apache.zookeeper.data.*;
import org.apache.zookeeper.KeeperException.Code;

public class DistProcess implements Runnable, Watcher, IDistComponent {
	ZooKeeper zk;
	String zkServer, pinfo;
	boolean isMaster = false;
	IDistComponent distComponent;

	DistProcess(String zkhost) {
		zkServer = zkhost;
		pinfo = ManagementFactory.getRuntimeMXBean().getName();
		System.out.println("DISTAPP : ZK Connection information : " + zkServer);
		System.out.println("DISTAPP : Process information : " + pinfo);
	}

	@Override
	public void run() {
		try{
			startProcess();
			while(true){
				Thread.sleep(1);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	//Watchers
	@Override
	public void process(WatchedEvent event) {
		System.out.println("DISTAPP : Event received : " + event);
		if(distComponent == null){
			System.out.println("DISTAPP : distComponent not ready...");
			return;
		}
		distComponent.process(event);
	}

	//getChildren aync callback
	@Override
	public void processResult(int rc, String path, Object ctx, List<String> children) {
		System.out.println("DISTAPP : Callback: getChildren");

		if(distComponent == null){
			System.out.println("DISTAPP : distComponent not ready...");
			return;
		}
		
		distComponent.processResult(rc, path, ctx, children);
	}

	//getData async callback
	@Override
	public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
		System.out.println("DISTAPP : Callback: getData");

		if(distComponent == null){
			System.out.println("DISTAPP : distComponent not ready...");
			return;
		}
		distComponent.processResult(rc, path, ctx, data, stat);
	}

	void startProcess() throws IOException, UnknownHostException, KeeperException, InterruptedException {
		zk = new ZooKeeper(zkServer, 1000, this); // connect to ZK.
		try {
			isMaster = true;
			runForMaster(); // See if you can become the master (i.e, no other master exists)
		} catch (NodeExistsException nee) {
			isMaster = false;
		}
		distComponent = isMaster ? new DistMaster(zk): new DistWorker(zk);
	}

	// Try to become the master.
	void runForMaster() throws KeeperException, UnknownHostException, InterruptedException
	{
		//Try to create an ephemeral node to be the master, put the hostname and pid of this process as the data.
		// This is an example of Synchronous API invocation as the function waits for the execution and no callback is involved..
		zk.create("/dist34/master", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}


	public static void main(String args[]) throws Exception {
		// Create a new process
		// Read the ZooKeeper ensemble information from the environment variable.
		DistProcess dt = new DistProcess(System.getenv("ZKSERVER"));
		Thread thread = new Thread(dt);
		thread.run();
	}

	@Override
	public void processResult(int rc, String path, Object ctx, Stat stat) {
		//Do nothing
	}
}
