package ch.admin.bar.siard2.jdbc;

import org.junit.Test;

public class ParseAccessTester
{

  @Test
  public void testParser()
  {
    String sSql = "SELECT \r\n" + 
      "  IIf(\r\n" + 
      "    IsNull(\"Last Name\"),\r\n" + 
      "    IIf(\r\n" + 
      "      IsNull(\"First Name\"),\r\n" + 
      "      \"Company\",\r\n" + 
      "      \"First Name\"\r\n" + 
      "    ),\r\n" + 
      "    IIf(\r\n" + 
      "      IsNull(\"First Name\"),\r\n" + 
      "      \"Last Name\",\r\n" + 
      "      \"Last Name\" || ', ' || \"First Name\"\r\n" + 
      "    )\r\n" + 
      "  ) AS \"File As\", \r\n" + 
      "  IIf(\r\n" + 
      "    IsNull(\"Last Name\"),\r\n" + 
      "    IIf(\r\n" + 
      "      IsNull(\"First Name\"),\r\n" + 
      "      \"Company\",\r\n" + 
      "      \"First Name\"\r\n" + 
      "    ),\r\n" + 
      "    IIf(\r\n" + 
      "      IsNull(\"First Name\"),\r\n" + 
      "      \"Last Name\",\r\n" + 
      "      \"First Name\" || ' ' || \"Last Name\"\r\n" + 
      "    )\r\n" + 
      "  ) AS \"Contact Name\", Suppliers.*\r\n" + 
      "FROM Suppliers";
    String sParam = "\\s*([^\\)\\(]+?)\\s*";
    //String sIsNull = "(?i)IsNull\\(([^\\)\\(]*)\\)";
    String sIsNull = "(?i)IsNull\\("+sParam+"\\)";
    sSql = sSql.replaceAll(sIsNull, "$1 IS NULL");
    System.out.println(sSql);
    String sIIf = "(?i)IIf\\("+sParam+","+sParam+","+sParam+"\\)";
    for (String s = sSql.replaceAll(sIIf, "CASE WHEN $1 THEN $2 ELSE $3 END");
      !s.equals(sSql);
      s = sSql.replaceAll(sIIf, "CASE WHEN $1 THEN $2 ELSE $3 END"))
    {
      sSql = s;
      System.out.println(sSql);
    }
  }
  
}
