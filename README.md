Typo
=====

Typo is a simple serialization layer for Accumulo that makes it easy to read
and write java objects directly to Accumulo fields.  Typo serializes Java types
in such a way that the lexicographic sort order corresponds to the object sort
order.  Typo is not an ORM layer, its purpose is to make it easy to read and
write Java objects to the Accumulo key fields and value that sort correctly.

Below is a simple example of reading and writing data to Accumulo using Typo.

```java
// An easy way to use Typo is to create a class that extends it and use the 
// subtype everywhere in your code.  This is what was done below. Nomrally the
// class below would be public and in its own file.  To keep the example self 
// contained, this was not done.

class MyTypo extends Typo<Long,String,Double,String> {
  public MyTypo() {
    super(new LongLexicoder(), new StringLexicoder(), new DoubleLexicoder(), new StringLexicoder());
  }
}


// If you would like to create a formatter for the Accumulo shell then create a
// class like the following. This class and Typo will then need to be placed on
// the Accumulo classpath.

class MyFormatter extends TypoFormatter {
  public MyFormatter() {
    super(new MyTypo());
  }
}

/*
 * A Typo Constraint can also be created like the Formatter was.
 */

class MyConstraint extends TypoConstraint {
  public MyConstraint() {
    super(new MyTypo());
  }
}

/**
 * A simple example that reads from and write to Accumulo using Typo.
 */

public class TypoExample {
  public static void main(String[] args) throws Exception {
    MockInstance mi = new MockInstance();
    Connector conn = mi.getConnector("root", "secret");
    conn.tableOperations().create("foo");
    
    insertData(conn);
    scanData(conn);
  }
  
  /*
   * Insert data using java types
   */
  static void insertData(Connector conn) throws Exception {
    BatchWriter bw = conn.createBatchWriter("foo", 1000000, 60000, 2);
    
    MyTypo myTypo = new MyTypo();
    
    for (long row = -4; row < 4; row++) {
      MyTypo.Mutation mut = myTypo.newMutation(row);
      mut.put("sq", Math.pow(row, 2), "val");
      mut.put("cube", Math.pow(row, 3), "val");
      bw.addMutation(mut);
    }
    
    bw.close();
  }
  
  static void scanData(Connector conn) throws Exception {
    MyTypo myTypo = new MyTypo();
    Scanner scanner = conn.createScanner("foo", Constants.NO_AUTHS);
    
    // you can create a range using java types
    scanner.setRange(myTypo.newRange(-2l, 3l));
    
    MyTypo.Scanner typoScanner = myTypo.newScanner(scanner);
    
    // you can fetch columns using Java types
    typoScanner.fetchColumnFamily("sq");

    long rowSum = 0;
    double cqSum = 0;
    
    // read data from Accumulo using java types
    for (Entry<MyTypo.Key,String> entry : typoScanner) {
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

The souce code contains [additional examples](https://github.com/keith-turner/typo/tree/master/src/main/java/org/apache/accumulo/typo/example).
