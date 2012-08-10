Typo
=====

Typo is a simple serialization layer for Accumulo that makes it easy to read
and write java objects directly to Accumulo fields.  Typo serializes Java types
in such a way that the lexicographic sort order corresponds to the object sort
order.  Typo is not an ORM layer, its purpose is to make it easy to read and
write Java objects to the Accumulo key fields and value that sort correctly.

This project is an experiment and its API may change drastically.

Below is a simple example of reading and writing data to Accumulo using Typo.

```java
class MyTypo extends Typo<Long,String,Double,String> {
  public MyTypo() {
    super(new LongEncoder(), new StringEncoder(), new DoubleEncoder(), new StringEncoder());
  }
}

public class TypoExample {
  public static void main(String[] args) throws Exception {
    MockInstance mi = new MockInstance();
    Connector conn = mi.getConnector("root", "secret");
    conn.tableOperations().create("foo");
    
    insertData(conn);
    scanData(conn);
  }
  
  static void insertData(Connector conn) throws Exception {
    BatchWriter bw = conn.createBatchWriter("foo", 1000000, 60000, 2);
    
    MyTypo myTypo = new MyTypo();
    
    for (long row = -4; row < 4; row++) {
      TypoMutation<Long,String,Double,String> mut = myTypo.newMutation(row);
      mut.put("sq", Math.pow(row, 2), "val");
      mut.put("cube", Math.pow(row, 3), "val");
      bw.addMutation(mut);
    }
    
    bw.close();
  }
  
  static void scanData(Connector conn) throws Exception {
    MyTypo myTypo = new MyTypo();
    Scanner scanner = conn.createScanner("foo", Constants.NO_AUTHS);
    
    scanner.setRange(myTypo.newRange(-2l, 3l));
    
    TypoScanner<Long,String,Double,String> typoScanner = myTypo.newScanner(scanner);
    
    typoScanner.fetchColumnFamily("sq");

    long rowSum = 0;
    double cqSum = 0;
    
    for (Entry<TypoKey<Long,String,Double>,String> entry : typoScanner) {
      rowSum += entry.getKey().getRow();
      cqSum += entry.getKey().getColumnQualifier();
      System.out.println(entry);
    }
    
    System.out.println("rowSum : " + rowSum);
    System.out.println("cqSum  : " + cqSum);
  }
}
```

The example outputs the following.

<pre>
-2 sq 4.0 [] 1344641128233 val
-1 sq 1.0 [] 1344641128234 val
0 sq 0.0 [] 1344641128234 val
1 sq 1.0 [] 1344641128234 val
2 sq 4.0 [] 1344641128234 val
3 sq 9.0 [] 1344641128234 val
rowSum : 3
cqSum  : 19.0
</pre>

