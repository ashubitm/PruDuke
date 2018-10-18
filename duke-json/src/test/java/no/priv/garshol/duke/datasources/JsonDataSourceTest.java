
package no.priv.garshol.duke.datasources;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import no.priv.garshol.duke.ConfigurationImpl;
import no.priv.garshol.duke.Processor;
import no.priv.garshol.duke.Property;
import no.priv.garshol.duke.PropertyImpl;
import no.priv.garshol.duke.Record;
import no.priv.garshol.duke.RecordIterator;
import no.priv.garshol.duke.comparators.Levenshtein;
import no.priv.garshol.duke.matchers.PrintMatchListener;
import no.priv.garshol.duke.utils.TestUtils;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by damien on 08/04/14.
 */
public class JsonDataSourceTest {
	 private ConfigurationImpl config;
	  private Processor processor;
  private JsonDataSource source;
  private JsonDataSource sourceEntry;
  private TestUtils.TestListener listener;

  @Before
  public void setup() {
	  listener = new TestUtils.TestListener();
    source = new JsonDataSource();
    sourceEntry = new JsonDataSource();
    Levenshtein comp = new Levenshtein();
    List<Property> props = new ArrayList();
    props.add(new PropertyImpl("F1"));
    props.add(new PropertyImpl("F2", comp, 0.3, 0.8));
    props.add(new PropertyImpl("F3", comp, 0.3, 0.8));

    config = new ConfigurationImpl();
    config.setProperties(props);
    config.setThreshold(0.85);
    config.setMaybeThreshold(0.8);

   // source1 = new InMemoryDataSource();
   // source2 = new InMemoryDataSource();
    config.addDataSource(0, sourceEntry);
    config.addDataSource(0, source);
   
    
    processor = new Processor(config, true);
    processor.addMatchListener(listener);

    source.addColumn(new Column("F1", null, null, null));
    source.addColumn(new Column("F2", null, null, null));
    source.addColumn(new Column("F3", null, null, null));
    System.out.println("Print the source JSON"+source);
  }

/*  @Test
  public void testEmpty() throws IOException {
    RecordIterator it = source.getRecordsFromString("");
    assertTrue(!it.hasNext());
  }

  @Test
  public void testSingleRecord() throws IOException {
    Record r = source.getRecordsFromString("{\"F1\":\"a\",\"F2\" : \"b\", \"F3\" : \"c\", \"F4\" : \"d\"}").next();

    System.out.println("############# Test single record##############"+r.getValue("F1"));
    assertEquals("a", r.getValue("F1"));
    assertEquals("b", r.getValue("F2"));
    assertEquals("c", r.getValue("F3"));
  }

  @Test
  public void testArrayField() {
    Record r = source.getRecordsFromString("{\"F1\":[\"a\",\"b\",\"c\"]}").next();
    assertEquals(3, r.getValues("F1").size());
  }

  @Test
  public void testNestRecords() {
    Record r = source.getRecordsFromString("{\"F1\":\"a\",\"FF2\" : {\"F2\" : \"b\"}, \"FFF3\" : {\"FF3\" : {\"F3\" : \"c\",\"F4\" : \"d\"}}}").next();
    assertEquals("a", r.getValue("F1"));
    assertEquals("b", r.getValue("F2"));
    assertEquals("c", r.getValue("F3"));
    System.out.println("############# Test Nest record##############"+r.getValue("F1"));
  }
  
  @Test
  public void multipleRecords() {
    RecordIterator it = source.getRecordsFromString("{\"F1\":\"a\",\"F2\" : \"b\", \"F3\" : \"c\"}{\"F1\":\"a2\",\"F2\" : \"b2\", \"F3\" : \"c2\"}");
    Record r1 = it.next();
    assertEquals("a", r1.getValue("F1"));
    assertEquals("b", r1.getValue("F2"));
    assertEquals("c", r1.getValue("F3"));
    System.out.println("############# Test multipleRecords record##############"+r1);
    Record r2 =  it.next();
    assertEquals("a2", r2.getValue("F1"));
    assertEquals("b2", r2.getValue("F2"));
    assertEquals("c2", r2.getValue("F3"));
    System.out.println("############# Test multipleRecords record##############"+r2);
  }
  
*/  
  @Test
  public void testContinuosStreams() throws IOException {
	  System.out.println("Entering testContinuosStreams ........");
    config.setMaybeThreshold(0.0);
    List<TestUtils.Pair> matches = listener.getMatches();
//    source.add(TestUtils.makeRecord("ID", "2", "NAME", "aaaaa", "EMAIL", "bbbbb"));
//    source2.add(TestUtils.makeRecord("ID", "3", "NAME", "xxxxx", "EMAIL", "bbbbb"));
    
  //stream method from APIs
  		String sentence = "Program aaaa bbbbb";
  		Stream<String> wordStream1 = Pattern.compile("\\W").splitAsStream(sentence);
  		String[] wordArr = wordStream1.toArray(String[]::new);
  		System.out.println(Arrays.toString(wordArr));
  		System.out.println("Stream in words  :"+wordArr[0]+wordArr[1]+wordArr[2]);
    
  	    Record r = source.getRecordsFromString("{\"F1\":\"a\",\"F2\" : \"b\", \"F3\" : \"c\", \"F4\" : \"d\"}").next();

  	    System.out.println("############# Test single record##############"+r.getValue("F1"));

 //   source1.add(TestUtils.makeRecord("ID", wordArr[0], "NAME",wordArr[1] , "EMAIL", wordArr[2]));
    
  		sourceEntry.addColumn(new Column("F1", "a", null, null));
  //  source1.add(TestUtils.makeRecord("ID", "2", "NAME", "aaaaa", "EMAIL", "bbbbb"));
    
    System.out.println("RecordLinkTest.testContinuosStreams: source1:"+source.toString());
    System.out.println("RecordLinkTest.testContinuosStreams: source2:"+sourceEntry.toString());
 //   processor.link();
    // COnvert to input steam
    //InputStream
    processor.deduplicate();
 //   processor.deduplicate(records);
    System.out.println("Deduplication complete");
    matches = listener.getMatches();
    System.out.println("################Bad record count #########"+matches.size());
  }
 /* 
  @Test
  public void testContinuosStreams() throws IOException {
	    listener = new TestUtils.TestListener();
	    Levenshtein comp = new Levenshtein();
	    List<Property> props = new ArrayList();
	    props.add(new PropertyImpl("ID",comp, 0.48, 0.99));
	    props.add(new PropertyImpl("NAME", comp, 0.49, 0.99));
	    props.add(new PropertyImpl("EMAIL", comp, 0.48, 0.99));

	    config = new ConfigurationImpl();
	    config.setProperties(props);
	    config.setThreshold(0.85);
	    config.setMaybeThreshold(0.8);

	    source1 = new InMemoryDataSource();
	    source2 = new InMemoryDataSource();
	   

	  config.setMaybeThreshold(0.0);
    List<TestUtils.Pair> matches = listener.getMatches();
    source2.add(TestUtils.makeRecord("ID", "2", "NAME", "aaaaa", "EMAIL", "bbbbb"));
    source2.add(TestUtils.makeRecord("ID", "3", "NAME", "xxxxx", "EMAIL", "yui"));
    
  //stream method from APIs
  		String sentence = "2 aaaaa bbbbb";
  		Stream<String> wordStream1 = Pattern.compile("\\W").splitAsStream(sentence);
  		String[] wordArr = wordStream1.toArray(String[]::new);
  		System.out.println(Arrays.toString(wordArr));
  		System.out.println("Stream in words  :"+wordArr[0]+wordArr[1]+wordArr[2]);
    
    source1.add(TestUtils.makeRecord("ID", wordArr[0], "NAME",wordArr[1] , "EMAIL", wordArr[2]));
    
  //  source1.add(TestUtils.makeRecord("ID", "2", "NAME", "aaaaa", "EMAIL", "bbbbb"));
    
    System.out.println("RecordLinkTest.testContinuosStreams: source1:"+source1.toString());
    System.out.println("RecordLinkTest.testContinuosStreams: source2:"+source2);
    config.addDataSource(0, source1);
    config.addDataSource(0, source2);
    
    processor = new Processor(config, true);
    processor.addMatchListener(new PrintMatchListener(true, true, true, false,
            config.getProperties(),
            true));
  //  processor.addMatchListener(listener);
 //   processor.link();
    processor.deduplicate();
    matches = listener.getMatches();
    System.out.println("################Bad record count #########"+matches.size());

  //  assertEquals("bad record count", 2, listener.getRecordCount());
  
 //   assertEquals("bad number of matches", 1, matches.size());
//    assertEquals("bad number of missed matches", 1, listener.getNoMatchCount());

//    TestUtils.Pair pair = matches.get(0);
//    if (pair.r1.getValue("ID").equals("2")) {
//      Record r = pair.r1;
//      pair.r1 = pair.r2;
//      pair.r2 = r;
//    }
//
//    assertEquals("1", pair.r1.getValue("ID"));
//    assertEquals("2", pair.r2.getValue("ID"));
  }
*/  
  
}

