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

    private class Entry {
        public var k : long;
        public var v : long;
        public def this(a : long, b : long) {
            k = a;
            v = b;
        }
    }
    
    private var h : Rail[Entry];
    private var capacity : long;
    private var size : long;
	private var count : long;
	private var defaultV : long;

	public def this(defV : long, workers : long , ratio : double , ins_per_thread : long , key_limit : long , value_limit : long){
	    count = 0;
	    size = 0;
	    capacity = 128;
	    defaultV = defV;
	    h = new Rail[Entry](capacity);
	}
	
	public def hash(key : long) : long {
	    val k = key.hashCode();
	    val index = k & (this.capacity - 1);
	    return index;
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
        while (true) {
        	atomic {
            	if (h(i) == null) {
                	var newE : Entry = new Entry(key, value);
                	h(i) = newE;
                	++size;
                	assert (size <= capacity) : "Full";
                	++count;
                	return count;
            	} else if (this.h(i).k == key) {
                	h(i).v = value;
                	++count;
                	return count;
            	}
        	}
        	i = (i + 1) & (capacity - 1);
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
        while (true) {
            atomic {
                if (h(i) == null) {
                    ++count;
                    return new Pair[long, long](count, defaultV);
                } else if (h(i).k == key) {
                    ++count;
                    return new Pair[long, long](count, h(i).v);
                } else if (fail == capacity) {
                    ++count;
                    return new Pair[long, long](count, defaultV);
                }
            }
            ++fail;
            Console.OUT.println("Fail " + fail);
            i = (i + 1) & (capacity - 1);
        }
    }
}