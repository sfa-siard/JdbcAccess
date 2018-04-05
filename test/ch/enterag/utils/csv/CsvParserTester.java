package ch.enterag.utils.csv;

import java.io.*;
import static org.junit.Assert.*;
import org.junit.*;
import ch.enterag.utils.*;

public class CsvParserTester
{

  @Test
  public void test()
  {
    CsvParser cp = new CsvParserImpl(';');
    try
    {
      String[] as = cp.parseLine("\"D\"\"E\";\"FR\";\"IT\";\"EN\"");
      for (int i = 0; i < as.length; i++)
        System.out.println(as[i]);
      as = cp.parseLine("1;2;3;4");
      for (int i = 0; i < as.length; i++)
        System.out.println(as[i]);
      as = cp.parseLine("\"DE\";2;\"IT\";4");
      for (int i = 0; i < as.length; i++)
        System.out.println(as[i]);
    }
    catch(IOException ie) { fail(EU.getExceptionMessage(ie)); }
  }
  
}
