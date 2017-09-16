package com.timgroup.statsd;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.charset.Charset;
import java.util.concurrent.*;


/**
 * A simple StatsD client implementation facilitating metrics recording.
 *
 * <p>Upon instantiation, this client will establish a socket connection to a StatsD instance
 * running on the specified host and port. Metrics are then sent over this connection as they are
 * received by the client.
 * </p>
 *
 * <p>Three key methods are provided for the submission of data-points for the application under
 * scrutiny:
 * <ul>
 *   <li>{@link #incrementCounter} - adds one to the value of the specified named counter</li>
 *   <li>{@link #recordGaugeValue} - records the latest fixed value for the specified named gauge</li>
 *   <li>{@link #recordExecutionTime} - records an execution time in milliseconds for the specified named operation</li>
 *   <li>{@link #recordHistogramValue} - records a value, to be tracked with average, maximum, and percentiles</li>
 *   <li>{@link #recordEvent} - records an event</li>
 *   <li>{@link #recordSetValue} - records a value in a set</li>
 * </ul>
 * From the perspective of the application, these methods are non-blocking, with the resulting
 * IO operations being carried out in a separate thread. Furthermore, these methods are guaranteed
 * not to throw an exception which may disrupt application execution.
 *
 * <p>As part of a clean system shutdown, the {@link #stop()} method should be invoked
 * on any StatsD clients.</p>
 *
 * @author Tom Denley
 *
 */
public final class DisruptorStatsDClient implements StatsDClient {

    private static final int PACKET_SIZE_BYTES = 1400;

    private static final StatsDClientErrorHandler NO_OP_HANDLER = new StatsDClientErrorHandler() {
        @Override public void handle(final Exception e) { /* No-op */ }
    };

    
    
    
    private final String prefix;
    private final String constantTagsRendered;
    
    private final DatagramChannel clientChannel;
    private final StatsDClientErrorHandler handler;
    

    private final RingBuffer<StatsEvent> ringBuffer;
    private final Disruptor<StatsEvent> disruptor;
    
    /**
     * Create a new StatsD client communicating with a StatsD instance on the
     * specified host and port. All messages send via this client will have
     * their keys prefixed with the specified string. The new client will
     * attempt to open a connection to the StatsD server immediately upon
     * instantiation, and may throw an exception if that a connection cannot
     * be established. Once a client has been instantiated in this way, all
     * exceptions thrown during subsequent usage are consumed, guaranteeing
     * that failures in metrics will not affect normal code execution.
     *
     * @param prefix
     *     the prefix to apply to keys sent via this client
     * @param hostname
     *     the host name of the targeted StatsD server
     * @param port
     *     the port of the targeted StatsD server
     * @throws StatsDClientException
     *     if the client could not be started
     */
    public DisruptorStatsDClient(final String prefix, final String hostname, final int port) throws StatsDClientException {
        this(prefix, hostname, port, 1 << 18);
    }

    /**
     * Create a new StatsD client communicating with a StatsD instance on the
     * specified host and port. All messages send via this client will have
     * their keys prefixed with the specified string. The new client will
     * attempt to open a connection to the StatsD server immediately upon
     * instantiation, and may throw an exception if that a connection cannot
     * be established. Once a client has been instantiated in this way, all
     * exceptions thrown during subsequent usage are consumed, guaranteeing
     * that failures in metrics will not affect normal code execution.
     *
     * @param prefix
     *     the prefix to apply to keys sent via this client
     * @param hostname
     *     the host name of the targeted StatsD server
     * @param port
     *     the port of the targeted StatsD server
     * @param queueSize
     *     the maximum amount of unprocessed messages in the BlockingQueue.
     * @throws StatsDClientException
     *     if the client could not be started
     */
    public DisruptorStatsDClient(final String prefix, final String hostname, final int port, final int queueSize) throws StatsDClientException {
        this(prefix, hostname, port, queueSize, null, null);
    }

    /**
     * Create a new StatsD client communicating with a StatsD instance on the
     * specified host and port. All messages send via this client will have
     * their keys prefixed with the specified string. The new client will
     * attempt to open a connection to the StatsD server immediately upon
     * instantiation, and may throw an exception if that a connection cannot
     * be established. Once a client has been instantiated in this way, all
     * exceptions thrown during subsequent usage are consumed, guaranteeing
     * that failures in metrics will not affect normal code execution.
     *
     * @param prefix
     *     the prefix to apply to keys sent via this client
     * @param hostname
     *     the host name of the targeted StatsD server
     * @param port
     *     the port of the targeted StatsD server
     * @param constantTags
     *     tags to be added to all content sent
     * @throws StatsDClientException
     *     if the client could not be started
     */
    public DisruptorStatsDClient(final String prefix, final String hostname, final int port, final String... constantTags) throws StatsDClientException {
        this(prefix, hostname, port, Integer.MAX_VALUE, constantTags, null);
    }

    /**
     * Create a new StatsD client communicating with a StatsD instance on the
     * specified host and port. All messages send via this client will have
     * their keys prefixed with the specified string. The new client will
     * attempt to open a connection to the StatsD server immediately upon
     * instantiation, and may throw an exception if that a connection cannot
     * be established. Once a client has been instantiated in this way, all
     * exceptions thrown during subsequent usage are consumed, guaranteeing
     * that failures in metrics will not affect normal code execution.
     *
     * @param prefix
     *     the prefix to apply to keys sent via this client
     * @param hostname
     *     the host name of the targeted StatsD server
     * @param port
     *     the port of the targeted StatsD server
     * @param constantTags
     *     tags to be added to all content sent
     * @param queueSize
     *     the maximum amount of unprocessed messages in the BlockingQueue.
     * @throws StatsDClientException
     *     if the client could not be started
     */
    public DisruptorStatsDClient(final String prefix, final String hostname, final int port, final int queueSize, final String... constantTags) throws StatsDClientException {
        this(prefix, hostname, port, queueSize, constantTags, null);
    }

    /**
     * Create a new StatsD client communicating with a StatsD instance on the
     * specified host and port. All messages send via this client will have
     * their keys prefixed with the specified string. The new client will
     * attempt to open a connection to the StatsD server immediately upon
     * instantiation, and may throw an exception if that a connection cannot
     * be established. Once a client has been instantiated in this way, all
     * exceptions thrown during subsequent usage are passed to the specified
     * handler and then consumed, guaranteeing that failures in metrics will
     * not affect normal code execution.
     *
     * @param prefix
     *     the prefix to apply to keys sent via this client
     * @param hostname
     *     the host name of the targeted StatsD server
     * @param port
     *     the port of the targeted StatsD server
     * @param constantTags
     *     tags to be added to all content sent
     * @param errorHandler
     *     handler to use when an exception occurs during usage, may be null to indicate noop
     * @throws StatsDClientException
     *     if the client could not be started
     */
    public DisruptorStatsDClient(final String prefix, final String hostname, final int port,
                                 final String[] constantTags, final StatsDClientErrorHandler errorHandler) throws StatsDClientException {
        this(prefix, Integer.MAX_VALUE, constantTags, errorHandler, staticStatsDAddressResolution(hostname, port));
    }

    /**
     * Create a new StatsD client communicating with a StatsD instance on the
     * specified host and port. All messages send via this client will have
     * their keys prefixed with the specified string. The new client will
     * attempt to open a connection to the StatsD server immediately upon
     * instantiation, and may throw an exception if that a connection cannot
     * be established. Once a client has been instantiated in this way, all
     * exceptions thrown during subsequent usage are passed to the specified
     * handler and then consumed, guaranteeing that failures in metrics will
     * not affect normal code execution.
     *
     * @param prefix
     *     the prefix to apply to keys sent via this client
     * @param hostname
     *     the host name of the targeted StatsD server
     * @param port
     *     the port of the targeted StatsD server
     * @param constantTags
     *     tags to be added to all content sent
     * @param errorHandler
     *     handler to use when an exception occurs during usage, may be null to indicate noop
     * @param queueSize
     *     the maximum amount of unprocessed messages in the BlockingQueue.
     * @throws StatsDClientException
     *     if the client could not be started
     */
    public DisruptorStatsDClient(final String prefix, final String hostname, final int port, final int queueSize,
                                 final String[] constantTags, final StatsDClientErrorHandler errorHandler) throws StatsDClientException {
        this(prefix, queueSize, constantTags, errorHandler, staticStatsDAddressResolution(hostname, port));
    }

    /**
     * Create a new StatsD client communicating with a StatsD instance on the
     * specified host and port. All messages send via this client will have
     * their keys prefixed with the specified string. The new client will
     * attempt to open a connection to the StatsD server immediately upon
     * instantiation, and may throw an exception if that a connection cannot
     * be established. Once a client has been instantiated in this way, all
     * exceptions thrown during subsequent usage are passed to the specified
     * handler and then consumed, guaranteeing that failures in metrics will
     * not affect normal code execution.
     *
     * @param prefix
     *     the prefix to apply to keys sent via this client
     * @param constantTags
     *     tags to be added to all content sent
     * @param errorHandler
     *     handler to use when an exception occurs during usage, may be null to indicate noop
     * @param addressLookup
     *     yields the IP address and socket of the StatsD server
     * @param queueSize
     *     the maximum amount of unprocessed messages in the BlockingQueue.
     * @throws StatsDClientException
     *     if the client could not be started
     */
    public DisruptorStatsDClient(final String prefix, final int queueSize, String[] constantTags, final StatsDClientErrorHandler errorHandler,
                                 final Callable<InetSocketAddress> addressLookup) throws StatsDClientException {
        if((prefix != null) && (!prefix.isEmpty())) {
            this.prefix = String.format("%s.", prefix);
        } else {
            this.prefix = "";
        }
        if(errorHandler == null) {
            handler = NO_OP_HANDLER;
        }
        else {
            handler = errorHandler;
        }

        /* Empty list should be null for faster comparison */
        if((constantTags != null) && (constantTags.length == 0)) {
            constantTags = null;
        }

        if(constantTags != null) {
            constantTagsRendered = StatsEvent.tagString(constantTags, null);
        } else {
            constantTagsRendered = null;
        }

        try {
            clientChannel = DatagramChannel.open();
        } catch (final Exception e) {
            throw new StatsDClientException("Failed to start StatsD client", e);
        }
        
        /*
            public Disruptor(EventFactory<T> eventFactory, int ringBufferSize, ThreadFactory threadFactory, ProducerType producerType, WaitStrategy waitStrategy) {

         */
    
        ThreadFactory threadFactory = new ThreadFactory() {
            final ThreadFactory delegate = Executors.defaultThreadFactory();
        
            @Override
            public Thread newThread(final Runnable r) {
                final Thread result = delegate.newThread(r);
                result.setName("StatsD-" + result.getName());
                result.setDaemon(true);
                return result;
            }
        };
    
        WaitStrategy waitStrategy = new SleepingWaitStrategy();
        StatsEventFactory factory = new StatsEventFactory();
        int bufferSize = Pow2.roundToPowerOfTwo(queueSize);
        this.disruptor = new Disruptor<StatsEvent>(factory, bufferSize, threadFactory, ProducerType.MULTI, waitStrategy);
        
        disruptor.handleEventsWith(new StatsEventHandler(addressLookup));
        disruptor.start();
        
        ringBuffer = disruptor.getRingBuffer();
    }

    /**
     * Cleanly shut down this StatsD client. This method may throw an exception if
     * the socket cannot be closed.
     */
    @Override
    public void stop() {
        try {
            disruptor.shutdown(30, TimeUnit.SECONDS);
        }
        catch (final Exception e) {
            handler.handle(e);
        }
        finally {
            if (clientChannel != null) {
                try {
                    clientChannel.close();
                }
                catch (final IOException e) {
                    handler.handle(e);
                }
            }
        }
    }

    @Override
    public void close() {
        stop();
    }

  

    /**
     * Adjusts the specified counter by a given delta.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the counter to adjust
     * @param delta
     *     the amount to adjust the counter by
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void count(final String aspect, final long delta, final String... tags) {
        long sequence = ringBuffer.next();
        try {
            StatsEvent event = ringBuffer.get(sequence);
            event.type = StatsEvent.Type.count1;
            event.aspect = aspect;
            event.delta = delta;
            event.tags = tags;
        }
        finally {
            ringBuffer.publish(sequence);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void count(final String aspect, final long delta, final double sampleRate, final String...tags) {
    	if(isInvalidSample(sampleRate)) {
    		return;
    	}
    
        long sequence = ringBuffer.next();
        try {
            StatsEvent event = ringBuffer.get(sequence);
            event.type = StatsEvent.Type.count2;
            event.aspect = aspect;
            event.delta = delta;
            event.sampleRate = sampleRate;
            event.tags = tags;
        }
        finally {
            ringBuffer.publish(sequence);
        }
    }

    /**
     * Increments the specified counter by one.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the counter to increment
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void incrementCounter(final String aspect, final String... tags) {
        count(aspect, 1, tags);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void incrementCounter(final String aspect, final double sampleRate, final String... tags) {
    	count(aspect, 1, sampleRate, tags);
    }

    /**
     * Convenience method equivalent to {@link #incrementCounter(String, String[])}.
     */
    @Override
    public void increment(final String aspect, final String... tags) {
        incrementCounter(aspect, tags);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void increment(final String aspect, final double sampleRate, final String...tags ) {
    	incrementCounter(aspect, sampleRate, tags);
    }

    /**
     * Decrements the specified counter by one.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the counter to decrement
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void decrementCounter(final String aspect, final String... tags) {
        count(aspect, -1, tags);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void decrementCounter(String aspect, final double sampleRate, final String... tags) {
        count(aspect, -1, sampleRate, tags);
    }

    /**
     * Convenience method equivalent to {@link #decrementCounter(String, String[])}.
     */
    @Override
    public void decrement(final String aspect, final String... tags) {
        decrementCounter(aspect, tags);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void decrement(final String aspect, final double sampleRate, final String... tags) {
        decrementCounter(aspect, sampleRate, tags);
    }

    /**
     * Records the latest fixed value for the specified named gauge.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the gauge
     * @param value
     *     the new reading of the gauge
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void recordGaugeValue(final String aspect, final double value, final String... tags) {
        /* Intentionally using %s rather than %f here to avoid
         * padding with extra 0s to represent precision */
        long sequence = ringBuffer.next();
        try {
            StatsEvent event = ringBuffer.get(sequence);
            event.type = StatsEvent.Type.gauge1;
            event.aspect = aspect;
            event.value = value;
            event.tags = tags;
        }
        finally {
            ringBuffer.publish(sequence);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void recordGaugeValue(final String aspect, final double value, final double sampleRate, final String... tags) {
    	if(isInvalidSample(sampleRate)) {
    		return;
    	}
        long sequence = ringBuffer.next();
        try {
            StatsEvent event = ringBuffer.get(sequence);
            event.type = StatsEvent.Type.gauge2;
            event.aspect = aspect;
            event.value = value;
            event.sampleRate = sampleRate;
            event.tags = tags;
        }
        finally {
            ringBuffer.publish(sequence);
        }
    }

    /**
     * Convenience method equivalent to {@link #recordGaugeValue(String, double, String[])}.
     */
    @Override
    public void gauge(final String aspect, final double value, final String... tags) {
        recordGaugeValue(aspect, value, tags);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void gauge(final String aspect, final double value, final double sampleRate, final String... tags) {
        recordGaugeValue(aspect, value, sampleRate, tags);
    }


    /**
     * Records the latest fixed value for the specified named gauge.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the gauge
     * @param value
     *     the new reading of the gauge
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void recordGaugeValue(final String aspect, final long value, final String... tags) {
        long sequence = ringBuffer.next();
        try {
            StatsEvent event = ringBuffer.get(sequence);
            event.type = StatsEvent.Type.gauge3;
            event.aspect = aspect;
            event.longValue = value;
            event.tags = tags;
        }
        finally {
            ringBuffer.publish(sequence);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void recordGaugeValue(final String aspect, final long value, final double sampleRate, final String... tags) {
    	if(isInvalidSample(sampleRate)) {
    		return;
    	}
    	
        long sequence = ringBuffer.next();
        try {
            StatsEvent event = ringBuffer.get(sequence);
            event.type = StatsEvent.Type.gauge4;
            event.aspect = aspect;
            event.longValue = value;
            event.sampleRate = sampleRate;
            event.tags = tags;
        }
        finally {
            ringBuffer.publish(sequence);
        }
    }

    /**
     * Convenience method equivalent to {@link #recordGaugeValue(String, long, String[])}.
     */
    @Override
    public void gauge(final String aspect, final long value, final String... tags) {
        recordGaugeValue(aspect, value, tags);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void gauge(final String aspect, final long value, final double sampleRate, final String... tags) {
        recordGaugeValue(aspect, value, sampleRate, tags);
    }

    /**
     * Records an execution time in milliseconds for the specified named operation.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the timed operation
     * @param timeInMs
     *     the time in milliseconds
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void recordExecutionTime(final String aspect, final long timeInMs, final String... tags) {
        long sequence = ringBuffer.next();
        try {
            StatsEvent event = ringBuffer.get(sequence);
            event.type = StatsEvent.Type.executionTime1;
            event.aspect = aspect;
            event.timeInMs = timeInMs;
            event.tags = tags;
        }
        finally {
            ringBuffer.publish(sequence);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void recordExecutionTime(final String aspect, final long timeInMs, final double sampleRate, final String... tags) {
    	if(isInvalidSample(sampleRate)) {
    		return;
    	}
        long sequence = ringBuffer.next();
        try {
            StatsEvent event = ringBuffer.get(sequence);
            event.type = StatsEvent.Type.executionTime2;
            event.aspect = aspect;
            event.timeInMs = timeInMs;
            event.sampleRate = sampleRate;
            event.tags = tags;
        }
        finally {
            ringBuffer.publish(sequence);
        }
    }

    /**
     * Convenience method equivalent to {@link #recordExecutionTime(String, long, String[])}.
     */
    @Override
    public void time(final String aspect, final long value, final String... tags) {
        recordExecutionTime(aspect, value, tags);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void time(final String aspect, final long value, final double sampleRate, final String... tags) {
        recordExecutionTime(aspect, value, sampleRate, tags);
    }

    /**
     * Records a value for the specified named histogram.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the histogram
     * @param value
     *     the value to be incorporated in the histogram
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void recordHistogramValue(final String aspect, final double value, final String... tags) {
        /* Intentionally using %s rather than %f here to avoid
         * padding with extra 0s to represent precision */
        long sequence = ringBuffer.next();
        try {
            StatsEvent event = ringBuffer.get(sequence);
            event.type = StatsEvent.Type.histogram1;
            event.aspect = aspect;
            event.value = value;
            event.tags = tags;
        }
        finally {
            ringBuffer.publish(sequence);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void recordHistogramValue(final String aspect, final double value, final double sampleRate, final String... tags) {
    	if(isInvalidSample(sampleRate)) {
    		return;
    	}
        long sequence = ringBuffer.next();
        try {
            StatsEvent event = ringBuffer.get(sequence);
            event.type = StatsEvent.Type.histogram2;
            event.aspect = aspect;
            event.value = value;
            event.sampleRate = sampleRate;
            event.tags = tags;
        }
        finally {
            ringBuffer.publish(sequence);
        }
    }

    /**
     * Convenience method equivalent to {@link #recordHistogramValue(String, double, String[])}.
     */
    @Override
    public void histogram(final String aspect, final double value, final String... tags) {
        recordHistogramValue(aspect, value, tags);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void histogram(final String aspect, final double value, final double sampleRate, final String... tags) {
        recordHistogramValue(aspect, value, sampleRate, tags);
    }

    /**
     * Records a value for the specified named histogram.
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the histogram
     * @param value
     *     the value to be incorporated in the histogram
     * @param tags
     *     array of tags to be added to the data
     */
    @Override
    public void recordHistogramValue(final String aspect, final long value, final String... tags) {
        long sequence = ringBuffer.next();
        try {
            StatsEvent event = ringBuffer.get(sequence);
            event.type = StatsEvent.Type.histogram3;
            event.aspect = aspect;
            event.longValue = value;
            event.tags = tags;
        }
        finally {
            ringBuffer.publish(sequence);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void recordHistogramValue(final String aspect, final long value, final double sampleRate, final String... tags) {
    	if(isInvalidSample(sampleRate)) {
    		return;
    	}
    
        long sequence = ringBuffer.next();
        try {
            StatsEvent event = ringBuffer.get(sequence);
            event.type = StatsEvent.Type.histogram4;
            event.aspect = aspect;
            event.longValue = value;
            event.sampleRate = sampleRate;
            event.tags = tags;
        }
        finally {
            ringBuffer.publish(sequence);
        }
    }

    /**
     * Convenience method equivalent to {@link #recordHistogramValue(String, long, String[])}.
     */
    @Override
    public void histogram(final String aspect, final long value, final String... tags) {
        recordHistogramValue(aspect, value, tags);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void histogram(final String aspect, final long value, final double sampleRate, final String... tags) {
        recordHistogramValue(aspect, value, sampleRate, tags);
    }

    /**
     * Records an event
     *
     * <p>This method is a DataDog extension, and may not work with other servers.</p>
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param event
     *     The event to record
     * @param tags
     *     array of tags to be added to the data
     *
     * @see <a href="http://docs.datadoghq.com/guides/dogstatsd/#events-1">http://docs.datadoghq.com/guides/dogstatsd/#events-1</a>
     */
    @Override
    public void recordEvent(final Event event, final String... tags) {
        long sequence = ringBuffer.next();
        try {
            StatsEvent statsEvent = ringBuffer.get(sequence);
            statsEvent.type = StatsEvent.Type.event1;
            statsEvent.event = event;
            statsEvent.tags = tags;
        }
        finally {
            ringBuffer.publish(sequence);
        }
    }


    /**
     * Records a run status for the specified named service check.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.</p>
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param sc
     *     the service check object
     */
    @Override
    public void recordServiceCheckRun(final ServiceCheck sc) {
        long sequence = ringBuffer.next();
        try {
            StatsEvent statsEvent = ringBuffer.get(sequence);
            statsEvent.type = StatsEvent.Type.serviceCheck;
            statsEvent.serviceCheck = sc;
        }
        finally {
            ringBuffer.publish(sequence);
        }
    }


    /**
     * Convenience method equivalent to {@link #recordServiceCheckRun(ServiceCheck sc)}.
     */
    @Override
    public void serviceCheck(final ServiceCheck sc) {
        recordServiceCheckRun(sc);
    }


    /**
     * Records a value for the specified set.
     *
     * Sets are used to count the number of unique elements in a group. If you want to track the number of
     * unique visitor to your site, sets are a great way to do that.
     *
     * <p>This method is a DataDog extension, and may not work with other servers.</p>
     *
     * <p>This method is non-blocking and is guaranteed not to throw an exception.</p>
     *
     * @param aspect
     *     the name of the set
     * @param value
     *     the value to track
     * @param tags
     *     array of tags to be added to the data
     *
     * @see <a href="http://docs.datadoghq.com/guides/dogstatsd/#sets">http://docs.datadoghq.com/guides/dogstatsd/#sets</a>
     */
    @Override
    public void recordSetValue(final String aspect, final String value, final String... tags) {
        // documentation is light, but looking at dogstatsd source, we can send string values
        // here instead of numbers
    
        long sequence = ringBuffer.next();
        try {
            StatsEvent event = ringBuffer.get(sequence);
            event.type = StatsEvent.Type.setValue;
            event.aspect = aspect;
            event.stringValue = value;
            event.tags = tags;
        }
        finally {
            ringBuffer.publish(sequence);
        }
    }

    private boolean isInvalidSample(double sampleRate) {
    	return sampleRate != 1 && Math.random() > sampleRate;
    }

    public static final Charset MESSAGE_CHARSET = Charset.forName("UTF-8");
    
    
    private class StatsEventHandler implements EventHandler<StatsEvent> {
        private final ByteBuffer sendBuffer = ByteBuffer.allocateDirect(PACKET_SIZE_BYTES);
        private final Callable<InetSocketAddress> addressLookup;
    
        public StatsEventHandler(Callable<InetSocketAddress> addressLookup) {
            this.addressLookup = addressLookup;
        }
    
        public void onEvent(StatsEvent event, long sequence, boolean endOfBatch) {
            try {
                String message;
                
                switch (event.type) {
                    case count1:
                        message = event.count1();
                        break;
                    case count2:
                        message = event.count2();
                        break;
                    case gauge1:
                        message = event.gauge1();
                        break;
                    case gauge2:
                        message = event.gauge2();
                        break;
                    case gauge3:
                        message = event.gauge3();
                        break;
                    case gauge4:
                        message = event.gauge4();
                        break;
                    case setValue:
                        message = event.setValue();
                        break;
                    case histogram1:
                        message = event.histogram1();
                        break;
                    case histogram2:
                        message = event.histogram2();
                        break;
                    case histogram3:
                        message = event.histogram3();
                        break;
                    case histogram4:
                        message = event.histogram4();
                        break;
                    case event1:
                        message = event.event1();
                        break;
                    case serviceCheck:
                        message = event.serviceCheck();
                        break;
                    case executionTime1:
                        message = event.executionTime1();
                        break;
                    case executionTime2:
                        message = event.executionTime2();
                        break;
                    default:
                        throw new IllegalArgumentException("unknown event type " + event.type);
                }
                
                final InetSocketAddress address = addressLookup.call();
                final byte[] data = message.getBytes(MESSAGE_CHARSET);
                if(sendBuffer.remaining() < (data.length + 1)) {
                    blockingSend(address);
                }
                if(sendBuffer.position() > 0) {
                    sendBuffer.put( (byte) '\n');
                }
                sendBuffer.put(data);
                if(endOfBatch) {
                    blockingSend(address);
                }
                
            } catch (Exception e) {
                handler.handle(e);
            }
        }
    
        private void blockingSend(final InetSocketAddress address) throws IOException {
            final int sizeOfBuffer = sendBuffer.position();
            sendBuffer.flip();
            
            final int sentBytes = clientChannel.send(sendBuffer, address);
            sendBuffer.limit(sendBuffer.capacity());
            sendBuffer.rewind();
        
            if (sizeOfBuffer != sentBytes) {
                handler.handle(
                    new IOException(
                                       String.format(
                                           "Could not send entirely stat %s to host %s:%d. Only sent %d bytes out of %d bytes",
                                           sendBuffer.toString(),
                                           address.getHostName(),
                                           address.getPort(),
                                           sentBytes,
                                           sizeOfBuffer)));
            }
        }
    }
    
    /**
     * Create dynamic lookup for the given host name and port.
     *
     * @param hostname the host name of the targeted StatsD server
     * @param port     the port of the targeted StatsD server
     * @return a function to perform the lookup
     */
    public static Callable<InetSocketAddress> volatileAddressResolution(final String hostname, final int port) {
        return new Callable<InetSocketAddress>() {
            @Override public InetSocketAddress call() throws UnknownHostException {
                return new InetSocketAddress(InetAddress.getByName(hostname), port);
            }
        };
    }

  /**
   * Lookup the address for the given host name and cache the result.
   *
   * @param hostname the host name of the targeted StatsD server
   * @param port     the port of the targeted StatsD server
   * @return a function that cached the result of the lookup
   * @throws Exception if the lookup fails, i.e. {@link UnknownHostException}
   */
    public static Callable<InetSocketAddress> staticAddressResolution(final String hostname, final int port) throws Exception {
        final InetSocketAddress address = volatileAddressResolution(hostname, port).call();
        return new Callable<InetSocketAddress>() {
            @Override public InetSocketAddress call() {
                return address;
            }
        };
    }

    private static Callable<InetSocketAddress> staticStatsDAddressResolution(final String hostname, final int port) throws StatsDClientException {
        try {
            return staticAddressResolution(hostname, port);
        } catch (final Exception e) {
            throw new StatsDClientException("Failed to lookup StatsD host", e);
        }
    }
    
    private class StatsEventFactory implements EventFactory<StatsEvent> {
        public StatsEvent newInstance() {
            return new StatsEvent(prefix, constantTagsRendered);
        }
    }
}
