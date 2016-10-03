/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2015, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.jdbc;

import org.postgresql.core.BaseConnection;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXParseException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.SQLException;
import java.sql.SQLXML;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stax.StAXResult;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

public class PgSQLXML implements SQLXML {

  private final BaseConnection _conn;
  private String _data; // The actual data contained.
  private boolean _initialized; // Has someone assigned the data for this object?
  private boolean _active; // Is anyone in the process of loading data into us?
  private boolean _freed;

  private ByteArrayOutputStream _byteArrayOutputStream;
  private StringWriter _stringWriter;
  private DOMResult _domResult;

  public PgSQLXML(BaseConnection conn) {
    this(conn, null, false);
  }

  public PgSQLXML(BaseConnection conn, String data) {
    this(conn, data, true);
  }

  private PgSQLXML(BaseConnection conn, String data, boolean initialized) {
    _conn = conn;
    _data = data;
    _initialized = initialized;
    _active = false;
    _freed = false;
  }

  public synchronized void free() {
    _freed = true;
    _data = null;
  }

  public synchronized InputStream getBinaryStream() throws SQLException {
    checkFreed();
    ensureInitialized();

    if (_data == null) {
      return null;
    }

    try {
      return new ByteArrayInputStream(_conn.getEncoding().encode(_data));
    } catch (IOException ioe) {
      // This should be a can't happen exception. We just
      // decoded this data, so it would be surprising that
      // we couldn't encode it.
      // For this reason don't make it translatable.
      throw new PSQLException("Failed to re-encode xml data.", PSQLState.DATA_ERROR, ioe);
    }
  }

  public synchronized Reader getCharacterStream() throws SQLException {
    checkFreed();
    ensureInitialized();

    if (_data == null) {
      return null;
    }

    return new StringReader(_data);
  }

  // We must implement this unsafely because that's what the
  // interface requires. Because it says we're returning T
  // which is unknown, none of the return values can satisfy it
  // as Java isn't going to understand the if statements that
  // ensure they are the same.
  //
  public synchronized <T extends Source> T getSource(Class<T> sourceClass) throws SQLException {
    checkFreed();
    ensureInitialized();

    if (_data == null) {
      return null;
    }

    try {
      if (sourceClass == null || DOMSource.class.equals(sourceClass)) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        builder.setErrorHandler(new NonPrintingErrorHandler());
        InputSource input = new InputSource(new StringReader(_data));
        return (T) new DOMSource(builder.parse(input));
      } else if (SAXSource.class.equals(sourceClass)) {
        InputSource is = new InputSource(new StringReader(_data));
        return (T) new SAXSource(is);
      } else if (StreamSource.class.equals(sourceClass)) {
        return (T) new StreamSource(new StringReader(_data));
      } else if (StAXSource.class.equals(sourceClass)) {
        XMLInputFactory xif = XMLInputFactory.newInstance();
        XMLStreamReader xsr = xif.createXMLStreamReader(new StringReader(_data));
        return (T) new StAXSource(xsr);
      }
    } catch (Exception e) {
      throw new PSQLException(GT.tr("Unable to decode xml data."), PSQLState.DATA_ERROR, e);
    }

    throw new PSQLException(GT.tr("Unknown XML Source class: {0}", sourceClass),
        PSQLState.INVALID_PARAMETER_TYPE);
  }

  public synchronized String getString() throws SQLException {
    checkFreed();
    ensureInitialized();
    return _data;
  }

  public synchronized OutputStream setBinaryStream() throws SQLException {
    checkFreed();
    initialize();
    _active = true;
    _byteArrayOutputStream = new ByteArrayOutputStream();
    return _byteArrayOutputStream;
  }

  public synchronized Writer setCharacterStream() throws SQLException {
    checkFreed();
    initialize();
    _stringWriter = new StringWriter();
    return _stringWriter;
  }

  public synchronized <T extends Result> T setResult(Class<T> resultClass) throws SQLException {
    checkFreed();
    initialize();

    if (resultClass == null || DOMResult.class.equals(resultClass)) {
      _domResult = new DOMResult();
      _active = true;
      return (T) _domResult;
    } else if (SAXResult.class.equals(resultClass)) {
      try {
        SAXTransformerFactory transformerFactory =
            (SAXTransformerFactory) SAXTransformerFactory.newInstance();
        TransformerHandler transformerHandler = transformerFactory.newTransformerHandler();
        _stringWriter = new StringWriter();
        transformerHandler.setResult(new StreamResult(_stringWriter));
        _active = true;
        return (T) new SAXResult(transformerHandler);
      } catch (TransformerException te) {
        throw new PSQLException(GT.tr("Unable to create SAXResult for SQLXML."),
            PSQLState.UNEXPECTED_ERROR, te);
      }
    } else if (StreamResult.class.equals(resultClass)) {
      _stringWriter = new StringWriter();
      _active = true;
      return (T) new StreamResult(_stringWriter);
    } else if (StAXResult.class.equals(resultClass)) {
      _stringWriter = new StringWriter();
      try {
        XMLOutputFactory xof = XMLOutputFactory.newInstance();
        XMLStreamWriter xsw = xof.createXMLStreamWriter(_stringWriter);
        _active = true;
        return (T) new StAXResult(xsw);
      } catch (XMLStreamException xse) {
        throw new PSQLException(GT.tr("Unable to create StAXResult for SQLXML"),
            PSQLState.UNEXPECTED_ERROR, xse);
      }
    }

    throw new PSQLException(GT.tr("Unknown XML Result class: {0}", resultClass),
        PSQLState.INVALID_PARAMETER_TYPE);
  }

  public synchronized void setString(String value) throws SQLException {
    checkFreed();
    initialize();
    _data = value;
  }

  private void checkFreed() throws SQLException {
    if (_freed) {
      throw new PSQLException(GT.tr("This SQLXML object has already been freed."),
          PSQLState.OBJECT_NOT_IN_STATE);
    }
  }

  private void ensureInitialized() throws SQLException {
    if (!_initialized) {
      throw new PSQLException(
          GT.tr(
              "This SQLXML object has not been initialized, so you cannot retrieve data from it."),
          PSQLState.OBJECT_NOT_IN_STATE);
    }

    // Is anyone loading data into us at the moment?
    if (!_active) {
      return;
    }

    if (_byteArrayOutputStream != null) {
      try {
        _data = _conn.getEncoding().decode(_byteArrayOutputStream.toByteArray());
      } catch (IOException ioe) {
        throw new PSQLException(GT.tr("Failed to convert binary xml data to encoding: {0}.",
            _conn.getEncoding().name()), PSQLState.DATA_ERROR, ioe);
      } finally {
        _byteArrayOutputStream = null;
        _active = false;
      }
    } else if (_stringWriter != null) {
      // This is also handling the work for Stream, SAX, and StAX Results
      // as they will use the same underlying stringwriter variable.
      //
      _data = _stringWriter.toString();
      _stringWriter = null;
      _active = false;
    } else if (_domResult != null) {
      // Copy the content from the result to a source
      // and use the identify transform to get it into a
      // friendlier result format.
      try {
        TransformerFactory factory = TransformerFactory.newInstance();
        Transformer transformer = factory.newTransformer();
        DOMSource domSource = new DOMSource(_domResult.getNode());
        StringWriter stringWriter = new StringWriter();
        StreamResult streamResult = new StreamResult(stringWriter);
        transformer.transform(domSource, streamResult);
        _data = stringWriter.toString();
      } catch (TransformerException te) {
        throw new PSQLException(GT.tr("Unable to convert DOMResult SQLXML data to a string."),
            PSQLState.DATA_ERROR, te);
      } finally {
        _domResult = null;
        _active = false;
      }
    }
  }


  private void initialize() throws SQLException {
    if (_initialized) {
      throw new PSQLException(
          GT.tr(
              "This SQLXML object has already been initialized, so you cannot manipulate it further."),
          PSQLState.OBJECT_NOT_IN_STATE);
    }
    _initialized = true;
  }

  // Don't clutter System.err with errors the user can't silence.
  // If something bad really happens an exception will be thrown.
  static class NonPrintingErrorHandler implements ErrorHandler {
    public void error(SAXParseException e) {
    }

    public void fatalError(SAXParseException e) {
    }

    public void warning(SAXParseException e) {
    }
  }

}

