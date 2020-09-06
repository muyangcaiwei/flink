import org.apache.flink.api.java.tuple.Tuple5;

public class SQLTB {
    public int id;
    public String tag;
    public int num;
    public String timed;
    public String name;
    public SQLTB(int id, String tag, int num, String timed, String name){
        this.id = id;
        this.tag = tag;
        this.num = num;
        this.timed = timed;
        this.name = name;
    }

    public String toString(){
        return "insert into flink_test values( " + id + ",\n" +
                tag + ",\n" +
                num + ",\n" +
                timed + ",\n" +
                name + "\n" +
                ")";
    }
    public Tuple5<Integer, String, Integer, String, String> toTuple(){
        return new Tuple5<Integer, String, Integer, String, String>(
                id, tag,num, timed, name
        );
    }
}
