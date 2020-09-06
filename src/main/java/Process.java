public class Process {
    public int id;
    public String tag;
    public int num;
    public String costTime;
    public boolean finished;
    public Process(int id, String tag, int num, String costTime, boolean finished){
        this.id = id;
        this.tag = tag;
        this.num = num;
        this.costTime = costTime;
        this.finished = finished;
    }
    public String toString(){
        return "id  = " + id + "\n" +
                "tag = " + tag + "\n" +
                "num = " + num + "\n" +
                "cost = " + costTime + "\n" +
                "finished = " + finished;
    }
}
