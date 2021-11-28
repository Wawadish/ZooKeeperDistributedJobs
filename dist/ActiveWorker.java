public class ActiveWorker {

    private byte[] data;
    private String taskPath;

    public ActiveWorker(byte[] data, String taskPath)
    {
        this.data = data;
        this.taskPath = taskPath;
    }

    public byte[] getData()
    {
        return data;
    }

    public String getTaskPath()
    {
        return taskPath;
    }
}