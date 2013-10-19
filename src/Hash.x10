import x10.util.Timer;
import x10.util.ArrayList;
import x10.util.HashMap;
import x10.util.Pair;
import x10.util.concurrent.Lock;
/**
 * This is the class that provides the HashMap functionalities.
 *
 * The assignment is to replace the content of this class with code that exhibit
 * a better scalability.
 */

class Entry {
    public var k : long;
    public var v : long;
    public var s : int;
}

public class Hash
{
    private var a : long = null;
	private var count : long;

	public def this(defV : long){
	    count = 0;
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
        return count;
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
        return new Pair[long, long](count, 0);
    }
}