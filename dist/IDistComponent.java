import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.Watcher;

public interface IDistComponent extends Watcher, AsyncCallback.ChildrenCallback, AsyncCallback.DataCallback, AsyncCallback.StatCallback{
}