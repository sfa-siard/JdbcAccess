package ch.admin.bar.siard2.jdbc;

import java.util.Arrays;

import org.junit.Test;

public class ParseAccessTester
{

  @Test
  public void testQueryParser1()
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
    System.out.println("\r\n"+sSql);
    String sIIf = "(?i)IIf\\("+sParam+","+sParam+","+sParam+"\\)";
    for (String s = sSql.replaceAll(sIIf, "CASE WHEN $1 THEN $2 ELSE $3 END");
      !s.equals(sSql);
      s = sSql.replaceAll(sIIf, "CASE WHEN $1 THEN $2 ELSE $3 END"))
    {
      sSql = s;
      System.out.println("\r\n"+sSql);
    }
  }
  
  @Test
  public void testQueryParser2()
  {
    String sSql = "SELECT\r\n" + 
      "  PRODUCTS.ID AS \"Product ID\",\r\n" + 
      "  PRODUCTS.\"Product Name\",\r\n" + 
      "  PRODUCTS.\"Product Code\",\r\n" + 
      "  NZ(\"Quantity Purchased\", 0) AS \"Qty Purchased\",\r\n" + 
      "  NZ(\"Quantity Sold\", 0) AS \"Qty Sold\",\r\n" + 
      "  NZ(\"Quantity On Hold\", 0) AS \"Qty On Hold\",\r\n" + 
      "  \"Qty Purchased\" - \"Qty Sold\" AS \"Qty On Hand\",\r\n" + 
      "  \"Qty Purchased\" - \"Qty Sold\" - \"Qty On Hold\" AS \"Qty Available\",\r\n" + 
      "  NZ(\"Quantity On Order\", 0) AS \"Qty On Order\",\r\n" + 
      "  NZ(\"Quantity On Back Order\", 0) AS \"Qty On Back Order\",\r\n" + 
      "  PRODUCTS.\"Reorder Level\",\r\n" + 
      "  PRODUCTS.\"Target Level\",\r\n" + 
      "  \"Target Level\" - \"Current Level\" AS \"Qty Below Target Level\",\r\n" + 
      "  \"Qty Available\" + \"Qty On Order\" - \"Qty On Back Order\" AS \"Current Level\",\r\n" + 
      "  CASE\r\n" + 
      "  WHEN \"Qty Below Target Level\" > 0 THEN CASE\r\n" + 
      "  WHEN \"Qty Below Target Level\" < \"Minimum ReOrder Quantity\" THEN \"Minimum Reorder Quantity\"\r\n" + 
      "  ELSE \"Qty Below Target Level\"\r\n" + 
      "END\r\n" + 
      "  ELSE 0\r\n" + 
      "END AS \"Qty To Reorder\"\r\n" + 
      "FROM PRODUCTS"; 
    String sParam = "\\s*([^\\)\\(]+?)\\s*";
    //String sIsNull = "(?i)IsNull\\(([^\\)\\(]*)\\)";
    String sIsNull = "(?i)IsNull\\("+sParam+"\\)";
    sSql = sSql.replaceAll(sIsNull, "$1 IS NULL");
    // System.out.println("\r\n"+sSql);
    String sIIf = "(?i)IIf\\("+sParam+","+sParam+","+sParam+"\\)";
    for (String s = sSql.replaceAll(sIIf, "CASE WHEN $1 THEN $2 ELSE $3 END");
      !s.equals(sSql);
      s = sSql.replaceAll(sIIf, "CASE WHEN $1 THEN $2 ELSE $3 END"))
      sSql = s;
    // System.out.println("\r\n"+sSql);
    String sNz = "(?i)NZ\\("+sParam+","+sParam+"\\)";
    String s = sSql.replaceAll(sNz, "CASE WHEN $1 IS NULL THEN $2 ELSE $1 END");
    System.out.println("\r\n"+s);
  }
  
  @Test
  public void testFromTablesParser()
  {
    String sTables = "(\r\n" + 
      "  (\r\n" + 
      "    (\r\n" + 
      "      (\r\n" + 
      "        Products \r\n" + 
      "        LEFT JOIN [Inventory Sold] ON Products.ID=[Inventory Sold].[Product ID]\r\n" + 
      "      ) \r\n" + 
      "      LEFT JOIN [Inventory Purchased] ON Products.ID=[Inventory Purchased].[Product ID]\r\n" + 
      "    ) \r\n" + 
      "    LEFT JOIN [Inventory On Hold] ON Products.ID=[Inventory On Hold].[Product ID]\r\n" + 
      "  ) \r\n" + 
      "  LEFT JOIN [Inventory On Order] ON Products.ID=[Inventory On Order].[Product ID]\r\n" + 
      ") LEFT JOIN [Products On Back Order] ON Products.ID=[Products On Back Order].[Product ID]\r\n";
    /* remove on conditions */
    String sOn = "(ON[^\\)]*)";
    sTables = sTables.replaceAll(sOn,"");
    /* remove parentheses */
    sTables = sTables.replace("(", "").replace(")","");
    /* remove all LEFT RIGHT INNER OUTER */
    sTables = sTables.replace("LEFT", "").replace("RIGHT","").replace("INNER", "").replace("OUTER", "");
    /* replace all JOINS by commas */
    sTables = sTables.replace("JOIN", ",");
    /* remove all white space outside brackets */
    boolean bInside = false;
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < sTables.length(); i++)
    {
      char c = sTables.charAt(i);
      if (c == ']')
        bInside = false;
      else if (c == '[')
        bInside = true;
      else if (!bInside)
      {
        if (!Character.isWhitespace(c))
          sb.append(c);
      }
      else
        sb.append(c);
    }
    sTables = sb.toString();
    String[] asTable = sTables.split(",");
    System.out.println(Arrays.toString(asTable));
  }
}
