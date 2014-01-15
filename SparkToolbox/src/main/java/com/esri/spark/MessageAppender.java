package com.esri.spark;

import com.esri.arcgis.geodatabase.IGPMessages;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.spi.LoggingEvent;

import java.io.IOException;

/**
 */
public class MessageAppender extends ConsoleAppender
{
    private IGPMessages m_messages;

    public synchronized void setGPMessages(final IGPMessages messages)
    {
        m_messages = messages;
    }

    @Override
    public void close()
    {

    }

    @Override
    public boolean requiresLayout()
    {
        return false;
    }

    @Override
    public void append(final LoggingEvent loggingEvent)
    {
        if (m_messages != null)
        {
            try
            {
                m_messages.addMessage(loggingEvent.getMessage().toString());
            }
            catch (IOException e)
            {
                // NOOP;
            }
        }
    }
}
