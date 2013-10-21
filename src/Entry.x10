public class Entry {
    public var k : long;
    public var v : long;
    public var lock : RWLock;
    public def this(a : long, b : long) {
        k = a;
        v = b;
        lock = new RWLock();
    }
}