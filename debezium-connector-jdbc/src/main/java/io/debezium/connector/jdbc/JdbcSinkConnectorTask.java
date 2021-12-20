/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static io.debezium.connector.jdbc.HibernateJaxbRecordMapper.HIBERNATE_XML_SUFFIX;
import static io.debezium.connector.jdbc.HibernateJaxbRecordMapper.RESOURCES_PATH;
import static io.debezium.connector.jdbc.HibernateJaxbRecordMapper.persistJaxbHbmHibernateMapping;
import static io.debezium.connector.jdbc.HibernateRecordParser.getDataMap;

import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import javax.persistence.EntityManager;
import javax.xml.bind.JAXBException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.boot.jaxb.hbm.spi.JaxbHbmHibernateMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.data.Envelope;

/**
 * The main task executing streaming from sink connector.
 * Responsible for lifecycle management of the streaming code.
 *
 * @author Hossein Torabi
 *
 */
public class JdbcSinkConnectorTask extends SinkTask {
    private static enum State {
        RUNNING,
        STOPPED;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSinkConnectorTask.class);

    private final AtomicReference<State> state = new AtomicReference<State>(State.STOPPED);
    private final ReentrantLock stateLock = new ReentrantLock();
    private final Map<String, Schema> topicSchema = new HashMap<>();

    private JdbcSinkConnectorConfig config;

    private SessionFactory sessionFactory;
    private EntityManager entityManager;
    private Session session;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        stateLock.lock();

        try {
            if (!state.compareAndSet(State.STOPPED, State.RUNNING)) {
                LOGGER.info("Connector has already been started");
                return;
            }

            config = new JdbcSinkConnectorConfig(props);
            config.validate();

            buildSessionFactory();

        }
        finally {
            stateLock.unlock();
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        try {
            session = entityManager.unwrap(Session.class);
            Transaction transaction = session.beginTransaction();
            for (SinkRecord record : records) {
                Struct value = (Struct) record.value();
                Schema valueSchema = record.valueSchema();
                String topic = record.topic();
                if (!Envelope.isEnvelopeSchema(valueSchema)) {
                    throw new RuntimeException("Only Debezium schema is supported");
                }

                Struct afterStruct = value.getStruct(Envelope.FieldName.AFTER);
                Schema afterSchema = afterStruct.schema();

                if (!topicSchema.containsKey(topic) || !topicSchema.get(topic).equals(afterSchema)) {
                    topicSchema.put(topic, afterSchema);
                    JaxbHbmHibernateMapping mapping = HibernateJaxbRecordMapper.getJaxbHbmHibernateMapping(topic, afterSchema);
                    try {
                        persistJaxbHbmHibernateMapping(topic, mapping);
                    }
                    catch (JAXBException | FileNotFoundException e) {
                        throw new RuntimeException("Can not persists new schema %s", e);
                    }
                    // commit old data
                    transaction.commit();
                    session.close();

                    buildSessionFactory();
                    session = entityManager.unwrap(Session.class);
                    transaction = session.beginTransaction();
                }

                Map<String, Object> dataMap;
                try {
                    dataMap = getDataMap(afterStruct);
                }
                catch (Exception e) {
                    throw new RuntimeException(String.format("Can not parse record value [%s]", record.value().toString()), e);
                }
                session.save(
                        record.topic(),
                        dataMap);

            }
            transaction.commit();

        }
        finally {
            session.close();
        }
    }

    /** Builds {@link SessionFactory}, {@link EntityManager} and {@link Session} from hibernate configuration
     * provided by {@link  JdbcSinkConnectorConfig#getHibernateConfiguration()}
     *
     */
    private void buildSessionFactory() {
        org.hibernate.cfg.Configuration hibernateConfig = config.getHibernateConfiguration();

        topicSchema.keySet().stream()
                .map(topic -> String.format("%s/%s.%s",
                        RESOURCES_PATH,
                        topic,
                        HIBERNATE_XML_SUFFIX))
                .forEach(hibernateConfig::addFile);

        sessionFactory = hibernateConfig.buildSessionFactory();
        entityManager = sessionFactory.createEntityManager();
    }

    @Override
    public void stop() {
        session.close();
        sessionFactory.close();
    }
}
