import x10.util.Timer;
import x10.util.ArrayList;
import x10.util.Pair;
import x10.util.concurrent.Lock;
/**
 * This is the class that provides the HashMap functionalities.
 *
 * The assignment is to replace the content of this class with code that exhibit
 * a better scalability.
 */

public class Hash
{

    private var h : Rail[Entry];
    private var capacity : long;
    private var size : long;
	private var count : long;
	private var count_lock : Lock;
	private var defaultV : long;

	public def this(defV : long, workers : long , ratio : double , ins_per_thread : long , key_limit : long , value_limit : long){
	    count = 0;
	    size = 0;
	    capacity = 1024;
	    defaultV = defV;
	    h = new Rail[Entry](capacity);
	    for (i in 0..(capacity - 1)) {
	        h(i) = new Entry(-1, -1);
	    }
	}
	
	private def hash(key : long) : long {
	    val k = key.hashCode();
	    val index = k & (this.capacity - 1);
	    return index;
	}
	
	private def probe(cur_idx : long) : long {
	    return (cur_idx + 1)%capacity;
	}

    /**
     * Insert the pair <key,value> in the hash table
     *     'key'
     *     'value' 
     *
     * This function return the unique order id of the operation in the linearized history.
     */
    public def put(key: long, value: long) : long
    {
        var i : long = hash(key);
        var order : long;
        while (true) {
            //Console.OUT.println("Put try: " + i + " with key " + key);
            h(i).lock.writeLock();
            if (h(i).k == -1) {
               // Console.OUT.println("Put new entry at : " + i + " with key " + key);
                order = ++count;
                h(i).k = key;
                h(i).v = value;
                h(i).lock.writeUnlock();
                return order;
            } else if (h(i).k == key) {
                //Console.OUT.println("Put update at : " + i + " with key " + key);
                order = ++count;
                h(i).v = value;
                h(i).lock.writeUnlock();
                return order;
            }
            //Console.OUT.println("Put Read " + i + " with key " + h(i).k);
            h(i).lock.writeUnlock();
        	i = probe(i);
        }
    }

    /**
     * get the value associated to the input key
     *     'key'
     *
     * This function return the pair composed by
	 *     'first'    unique order id of the operation in the linearized history.
	 *     'second'   values associated to the input pair (defaultValue if there is no value associated to the input key)
     */
    public def get(key: long) : Pair[long,long]
    {
        var i : long = hash(key);
        var fail : long = 0;
        var value : long;
        var order : long;
        while (true) {
            h(i).lock.readLock();
            //Console.OUT.println("Get try: " + i + " with key " + key);
            if (h(i).k == key) {
                //Console.OUT.println("Get at:" + i + " with key " + key);
                value = h(i).v;
                order = ++count;
                h(i).lock.readUnlock();
                return new Pair[long, long](order, value);
            } else if (h(i).k == -1) {
                //Console.OUT.println("Get failed at:" + i + " with key " + key);
                order = ++count;
                h(i).lock.readUnlock();
                return new Pair[long, long](order, defaultV);
            }
            //Console.OUT.println("Get Read " + i + " with key " + h(i).k);
            h(i).lock.readUnlock();
            i = probe(i);
        }
    }
}