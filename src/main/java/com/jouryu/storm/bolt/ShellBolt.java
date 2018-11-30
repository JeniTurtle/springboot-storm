package com.jouryu.storm.bolt;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import com.jouryu.utils.CustomShellProcess;

import org.apache.storm.generated.ShellComponent;
import org.apache.storm.task.IBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

/**
 * Created by tomorrow on 18/11/22.
 *
 * 自定义shellbolt
 *
 * 原理参考: https://blog.csdn.net/joeyon1985/article/details/41674991
 */


public class ShellBolt implements IBolt {
    public static Logger LOG = Logger.getLogger(ShellBolt.class);
    OutputCollector _collector;
    Map<String, Tuple> _inputs = new ConcurrentHashMap<>();

    private String[] _command;
    private CustomShellProcess _process;
    private volatile boolean _running = true;
    private volatile Throwable _exception;
    private LinkedBlockingQueue _pendingWrites = new LinkedBlockingQueue();
    private Random _rand;

    private Thread _readerThread;
    private Thread _writerThread;

    public ShellBolt(ShellComponent component) {
        this(component.get_execution_command(), component.get_script());
    }

    public ShellBolt(String... command) {
        _command = command;
    }

    public void prepare(Map stormConf, TopologyContext context,
                        final OutputCollector collector) {
        _rand = new Random();
        _process = new CustomShellProcess(_command);
        _collector = collector;

        try {
            //subprocesses must send their pid first thing
            Number subpid = _process.launch(stormConf, context, false);
            LOG.info("Launched subprocess with pid " + subpid);
        } catch (IOException e) {
            _process.destroy();
            throw new RuntimeException("Error when launching multilang subprocess\n" + _process.getErrorsString(), e);
        }

        // reader
        _readerThread = new Thread(new Runnable() {
            public void run() {
                while (_running) {
                    try {
                        JSONObject action = _process.readMessage();
                        if (action == null) {
                            // ignore sync
                        }
                        String command = (String) action.get("command");
                        if(command.equals("ack")) {
                            handleAck(action);
                        } else if (command.equals("fail")) {
                            handleFail(action);
                        } else if (command.equals("log")) {
                            String msg = (String) action.get("msg");
                            LOG.info("Shell msg: " + msg);
                        } else if (command.equals("emit")) {
                            handleEmit(action);
                        }
                    } catch (InterruptedException e) {

                    } catch (Throwable t) {
                        _process.destroy();
                        die(t);
                    }
                }
            }
        });

        _readerThread.start();

        _writerThread = new Thread(new Runnable() {
            public void run() {
                while (_running) {
                    try {
                        Object write = _pendingWrites.poll(1, SECONDS);
                        if (write != null) {
                            _process.writeMessage(write);
                        }
                    } catch (InterruptedException e) {
                    } catch (Throwable t) {
                        _process.destroy();
                        die(t);
                    }
                }
            }
        });

        _writerThread.start();
    }

    public void execute(Tuple input) {
        if (_exception != null) {
            throw new RuntimeException(_exception);
        }

        //just need an id
        String genId = Long.toString(_rand.nextLong());
        _inputs.put(genId, input);
        try {
            JSONObject obj = new JSONObject();
            obj.put("id", genId);
            obj.put("comp", input.getSourceComponent());
            obj.put("stream", input.getSourceStreamId());
            obj.put("task", input.getSourceTask());
            obj.put("tuple", input.getValues());
            _pendingWrites.put(obj);
        } catch(InterruptedException e) {
            _process.destroy();
            throw new RuntimeException("Error during multilang processing", e);
        }
    }

    public void cleanup() {
        _running = false;
        _process.destroy();
        _inputs.clear();
    }

    private void handleAck(Map action) {
        String id = (String) action.get("id");
        Tuple acked = _inputs.remove(id);
        if(acked==null) {
            throw new RuntimeException("Acked a non-existent or already acked/failed id: " + id);
        }
        _collector.ack(acked);
    }

    private void handleFail(Map action) {
        String id = (String) action.get("id");
        Tuple failed = _inputs.remove(id);
        if(failed==null) {
            throw new RuntimeException("Failed a non-existent or already acked/failed id: " + id);
        }
        _collector.fail(failed);
    }

    private void handleEmit(Map action) throws InterruptedException {
        String stream = (String) action.get("stream");
        if(stream==null) stream = Utils.DEFAULT_STREAM_ID;
        Long task = (Long) action.get("task");
        List<Object> tuple = (List) action.get("tuple");
        List<Tuple> anchors = new ArrayList<>();
        Object anchorObj = action.get("anchors");
        if(anchorObj!=null) {
            if(anchorObj instanceof String) {
                anchorObj = Arrays.asList(anchorObj);
            }
            for(Object o: (List) anchorObj) {
                Tuple t = _inputs.get((String) o);
                if (t == null) {
                    throw new RuntimeException("Anchored onto " + o + " after ack/fail");
                }
                anchors.add(t);
            }
        }
        if(task==null) {
            List<Integer> outtasks = _collector.emit(stream, anchors, tuple);
            Object need_task_ids = action.get("need_task_ids");
            if (need_task_ids == null || ((Boolean) need_task_ids).booleanValue()) {
                _pendingWrites.put(outtasks);
            }
        } else {
            _collector.emitDirect((int)task.longValue(), stream, anchors, tuple);
        }
    }

    private void die(Throwable exception) {
        _exception = exception;
    }
}