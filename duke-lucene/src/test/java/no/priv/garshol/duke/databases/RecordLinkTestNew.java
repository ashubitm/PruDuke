
package no.priv.garshol.duke.databases;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import no.priv.garshol.duke.ConfigLoader;
import no.priv.garshol.duke.Configuration;
import no.priv.garshol.duke.ConfigurationImpl;
import no.priv.garshol.duke.Processor;
import no.priv.garshol.duke.Property;
import no.priv.garshol.duke.PropertyImpl;
import no.priv.garshol.duke.Record;
import no.priv.garshol.duke.comparators.Levenshtein;
import no.priv.garshol.duke.datasources.InMemoryDataSource;
import no.priv.garshol.duke.matchers.MatchListener;
import no.priv.garshol.duke.matchers.PrintMatchListener;
import no.priv.garshol.duke.utils.TestUtils;
import org.apache.lucene.index.CorruptIndexException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

import static junit.framework.Assert.assertEquals;

public class RecordLinkTestNew {
  private ConfigurationImpl config;
  private Processor processor;
  private InMemoryDataSource source1;
  private InMemoryDataSource source2;
  private TestUtils.TestListener listener;
  
  @Before
  public void setup() throws CorruptIndexException, IOException {

  }

  @After
  public void cleanup() throws CorruptIndexException, IOException {
    processor.close();
  }
  
 /* @Test
  public void testEmpty() throws IOException {
    processor.link();
    assertEquals(0, listener.getMatches().size());
    assertEquals(0, listener.getRecordCount());
  }*/

 /* @Test
  public void testSimplePair() throws IOException {
    source1.add(TestUtils.makeRecord("ID", "1", "NAME", "aaaaa", "EMAIL", "bbbbb"));
    source2.add(TestUtils.makeRecord("ID", "2", "NAME", "aaaaa", "EMAIL", "bbbbb"));
    System.out.println("RecordLinkTest.testSimplePair:"+source1);
    System.out.println("RecordLinkTest.testSimplePair:"+source2);
    processor.link();

    assertEquals("bad record count", 1, listener.getRecordCount());
    List<TestUtils.Pair> matches = listener.getMatches();
    assertEquals("bad number of matches", 1, matches.size());

    TestUtils.Pair pair = matches.get(0);
    if (pair.r1.getValue("ID").equals("2")) {
      Record r = pair.r1;
      pair.r1 = pair.r2;
      pair.r2 = r;
    }

    assertEquals("1", pair.r1.getValue("ID"));
    assertEquals("2", pair.r2.getValue("ID"));
  }*/
/*
  @Test
  public void testOneMatchOneMiss() throws IOException {
    source1.add(TestUtils.makeRecord("ID", "1", "NAME", "aaaaa", "EMAIL", "bbbbb"));
    source2.add(TestUtils.makeRecord("ID", "2", "NAME", "aaaaa", "EMAIL", "bbbbb"));
    source2.add(TestUtils.makeRecord("ID", "3", "NAME", "xxxx", "EMAIL", "yyyyy"));
    processor.link();

    assertEquals("bad record count", 2, listener.getRecordCount());
    List<TestUtils.Pair> matches = listener.getMatches();
    assertEquals("bad number of matches", 1, matches.size());
    assertEquals("bad number of missed matches", 1, listener.getNoMatchCount());

    TestUtils.Pair pair = matches.get(0);
    if (pair.r1.getValue("ID").equals("2")) {
      Record r = pair.r1;
      pair.r1 = pair.r2;
      pair.r2 = r;
    }

    assertEquals("1", pair.r1.getValue("ID"));
    assertEquals("2", pair.r2.getValue("ID"));
  }

  @Test
  public void testOneMatchOneMiss2() throws IOException {
    config.setMaybeThreshold(0.0);
    source1.add(TestUtils.makeRecord("ID", "1", "NAME", "aaaaa", "EMAIL", "bbbbb"));
    source2.add(TestUtils.makeRecord("ID", "2", "NAME", "aaaaa", "EMAIL", "bbbbb"));
    source2.add(TestUtils.makeRecord("ID", "3", "NAME", "xxxxx", "EMAIL", "bbbbb"));
    System.out.println("RecordLinkTest.testOneMatchOneMiss2: source1:"+source1);
    System.out.println("RecordLinkTest.testOneMatchOneMiss2: source2:"+source2);
    processor.link();
 //   processor.dedupe();

    assertEquals("bad record count", 2, listener.getRecordCount());
    List<TestUtils.Pair> matches = listener.getMatches();
    assertEquals("bad number of matches", 1, matches.size());
    assertEquals("bad number of missed matches", 1, listener.getNoMatchCount());

    TestUtils.Pair pair = matches.get(0);
    if (pair.r1.getValue("ID").equals("2")) {
      Record r = pair.r1;
      pair.r1 = pair.r2;
      pair.r2 = r;
    }

    assertEquals("1", pair.r1.getValue("ID"));
    assertEquals("2", pair.r2.getValue("ID"));
  }
  
  
  @Test
  public void testContinuosStreams() throws IOException {
	  
	  Configuration config;
	try {
		config = ConfigLoader.load("C:\\Users\\SONY VAIO\\git\\Duke\\duke-lucene\\src\\main\\java\\config-databaseNew.xml");
	
	  processor = new Processor(config, true);
	    processor.addMatchListener(new PrintMatchListener(true, true, true, false,
	            config.getProperties(),
	            true));
	    listener = new TestUtils.TestListener();
	    
	} catch (SAXException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    processor.link();
   // PrintMatchListener matchesNew =null;
    Collection<MatchListener> matchlistners = processor.getListeners();
    
    
//  matches = listener.getMatches();
    PrintMatchListener printMatches =null;
    for(MatchListener matchesNew : matchlistners)
    {
    	printMatches =(PrintMatchListener)matchesNew;
  System.out.println("################Bad record count #########"+printMatches.getMatchCount()+"\n"+matchesNew.toString()+"no match count ");
  
  if(printMatches.getMatchCount()>0) {
	  //TODO If there is no match then only process it 
	  //IF there is a match for existing record do not process it /Update it 
  }
    }
    //  processor.deduplicate();
    

  }*/
  
   
  @Test
  public void testContinuosStreams() throws IOException {
	  
	  Configuration config;
	try {
		config = ConfigLoader.load("C:\\Users\\SONY VAIO\\git\\Duke\\duke-lucene\\src\\main\\java\\config-databaseNew.xml");
		  //TODO dedupe right now call being made for existing 2 files - ultimately pass this stream and dedupe from destination 
        Record r = TestUtils.makeRecord("ID", "1", "NAME", "J.RandomHacker","COMPANY","Main");
     
        Collection<Record> records = new HashSet();
        
        records.add(r);
	  processor = new Processor(config, true);
	    processor.addMatchListener(new PrintMatchListener(true, true, true, false,
	            config.getProperties(),
	            true));
	    listener = new TestUtils.TestListener();
	    

    processor.deduplicate(records);
   // PrintMatchListener matchesNew =null;
    Collection<MatchListener> matchlistners = processor.getListeners();
    
    
//  matches = listener.getMatches();
    PrintMatchListener printMatches =null;
    for(MatchListener matchesNew : matchlistners)
    {
    	printMatches =(PrintMatchListener)matchesNew;
  System.out.println("################Bad record count #########"+printMatches.getMatchCount()+"\n"+matchesNew.toString()+"no match count ");
  
  if(printMatches.getMatchCount()>0) {
	  //TODO If there is no match then only process it 
	  //IF there is a match for existing record do not process it /Update it 
  }
    }
    //  processor.deduplicate();
    
	} catch (SAXException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }
}