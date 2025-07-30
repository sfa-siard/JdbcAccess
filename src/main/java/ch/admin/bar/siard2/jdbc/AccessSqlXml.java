package ch.admin.bar.siard2.jdbc;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import java.io.*;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.util.Arrays;

public class AccessSqlXml implements SQLXML {
    private String _sContent = "";

    /** {@link SQLXML} */
    @Override
    public OutputStream setBinaryStream() throws SQLException {
        return new AsciiOutputStream(_sContent);
    } /* setBinaryStream */
    /*==================================================================*/

    /** {@link SQLXML} */
    @Override
    public Writer setCharacterStream() throws SQLException {
        return new SqlXmlWriter(_sContent);
    } /* setCharacterStream */
    /*==================================================================*/

    /*------------------------------------------------------------------*/

    /** {@link SQLXML} */
    @Override
    public <T extends Result> T setResult(Class<T> arg0)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("Result/Source not yet supported!");
    } /* setResult */

    /*------------------------------------------------------------------*/

    /** {@link SQLXML} */
    @Override
    public String getString() throws SQLException {
        return _sContent;
    } /* getString */

    /*------------------------------------------------------------------*/

    /** {@link SQLXML} */
    @Override
    public void setString(String str) throws SQLException {
        _sContent = str;
    } /* setString */

    /*------------------------------------------------------------------*/

    /** {@link SQLXML} */
    @Override
    public InputStream getBinaryStream() throws SQLException {
        return new ByteArrayInputStream(_sContent.getBytes());
    } /* getBinaryStream */

    /*------------------------------------------------------------------*/

    /** {@link SQLXML} */
    @Override
    public Reader getCharacterStream() throws SQLException {
        return new StringReader(_sContent);
    } /* getCharacterStream */


    /*------------------------------------------------------------------*/

    /** {@link SQLXML} */
    @Override
    public <T extends Source> T getSource(Class<T> arg0)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("Source/Result not yet supported!");
    } /* getSource */

    /*------------------------------------------------------------------*/

    /** {@link SQLXML} */
    @Override
    public void free() throws SQLException {
        _sContent = "";
    } /* free */

    /*------------------------------------------------------------------*/

    /*==================================================================*/
    private class SqlXmlWriter extends StringWriter {

        public SqlXmlWriter(String sInitialContent) {
            super();
            write(sInitialContent);
        } /* constructor */

        @Override
        public void close() throws IOException {
            super.close();
            /* copy content to string */
            _sContent = toString();
        } /* close */

    } /* class SqlXmlWriter */

    /*------------------------------------------------------------------*/

    private class AsciiOutputStream extends OutputStream {
        StringBuilder _sb = new StringBuilder();

        public AsciiOutputStream(String sInitialContent) {
            _sb.append(sInitialContent);
        } /* constructor */

        @Override
        public void write(int b) throws IOException {
            _sb.append((char) b);
        } /* write */

        @Override
        public void write(byte[] buf) {
            _sb.append(new String(buf));
        }

        @Override
        public void write(byte[] buf, int iOffset, int iLength) {
            _sb.append(new String(Arrays.copyOfRange(buf, iOffset, iOffset + iLength)));
        } /* write */

        @Override
        public void close() {
            _sContent = _sb.toString();
        } /* close */

    } /* class AsciiOutputStream */

} /* AccessSqlXml */
