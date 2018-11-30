package com.jouryu.utils;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Map;

import com.jouryu.common.CacheData;
import org.apache.commons.io.IOUtils;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import org.apache.storm.task.TopologyContext;

/**
 * Created by tomorrow on 18/11/22.
 */

public class CustomShellProcess {
    private DataOutputStream processIn;
    private BufferedReader processOut;
    private InputStream processErrorStream;
    private Process _subprocess;
    private String[] command;

    public CustomShellProcess(String[] command) {
        this.command = command;
    }

    public Number launch(Map conf, TopologyContext context,  boolean changeDirectory) throws IOException {
        ProcessBuilder builder = new ProcessBuilder(command);
        if(changeDirectory) {
            builder.directory(new File(context.getCodeDir()));
        }
        _subprocess = builder.start();
        CacheData.processList.add(_subprocess);

        processIn = new DataOutputStream(_subprocess.getOutputStream());
        processOut = new BufferedReader(new InputStreamReader(_subprocess.getInputStream()));
        processErrorStream = _subprocess.getErrorStream();

        JSONObject setupInfo = new JSONObject();
        setupInfo.put("pidDir", context.getPIDDir());
        setupInfo.put("conf", conf);
        setupInfo.put("context", context.toString());
        writeMessage(setupInfo);

        return (Number)readMessage().get("pid");
    }

    public void destroy() {
        _subprocess.destroy();
    }

    public void writeMessage(Object msg) throws IOException {
        String jsonStr = JSONValue.toJSONString(msg);
        writeString(jsonStr);
    }

    private void writeString(String str) throws IOException {
        byte[] strBytes = str.getBytes("UTF-8");
        processIn.write(strBytes, 0, strBytes.length);
        processIn.writeBytes("\nend\n");
        processIn.flush();
    }

    public JSONObject readMessage() throws IOException {
        String string = readString();
        JSONObject msg = (JSONObject)JSONValue.parse(string);
        if (msg != null) {
            return msg;
        } else {
            throw new IOException("unable to parse: " + string);
        }
    }

    public String getErrorsString() {
        if(processErrorStream!=null) {
            try {
                return IOUtils.toString(processErrorStream, Charset.defaultCharset());
            } catch(IOException e) {
                return "(Unable to capture error stream)";
            }
        } else {
            return "";
        }
    }

    private String readString() throws IOException {
        StringBuilder line = new StringBuilder();

        //synchronized (processOut) {
        while (true) {
            String subline = processOut.readLine();
            if(subline==null) {
                StringBuilder errorMessage = new StringBuilder();
                errorMessage.append("Pipe to subprocess seems to be broken!");
                if (line.length() == 0) {
                    errorMessage.append(" No output read.\n");
                }
                else {
                    errorMessage.append(" Currently read output: " + line.toString() + "\n");
                }
                errorMessage.append("Shell Process Exception:\n");
                errorMessage.append(getErrorsString() + "\n");
                throw new RuntimeException(errorMessage.toString());
            }
            if(subline.equals("end")) {
                break;
            }
            if(line.length()!=0) {
                line.append("\n");
            }
            line.append(subline);
        }
        //}

        return line.toString();
    }
}
