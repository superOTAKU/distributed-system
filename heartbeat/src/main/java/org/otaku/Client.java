package org.otaku;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class Client {
    private final String host;
    private final int port;
    private Socket socket;

    public Client(String host, int port) {
        this.host = host;
        this.port = port;
    }

    private class HbThread extends Thread {
        public HbThread() {
            setName("heartbeat");
            setDaemon(true);
        }

        @Override
        public void run() {
            try {
                while (true) {
                    Thread.sleep(3000);
                    write("HEARTBEAT");
                }
            } catch (Exception e) {
                System.err.println("heartbeat error");
                e.printStackTrace();
            }
        }
    }

    public void start() throws IOException {
        socket = new Socket(host, port);
        System.out.println("connected to " + socket.getRemoteSocketAddress());
        HbThread hbThread = new HbThread();
        hbThread.start();
        Scanner input = new Scanner(System.in);
        while (input.hasNextLine()) {
            String message = input.nextLine();
            if (message.equals("exit")) {
                System.out.println("good bye");
                break;
            } else {
                write(message);
                System.out.println("message sent");
            }
        }
    }

    public void close() {
        try {
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void write(String content) throws IOException {
        byte[] data = content.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeInt(data.length);
        out.write(data);
        //重点是，一次性地将整个buffer写入，防止心跳和普通消息并发，造成连续写两个长度包
        socket.getOutputStream().write(out.toByteArray());
    }

    public static void main(String[] args) {
        String host = "localhost";
        int port = 80;
        if (args.length >= 2) {
            host = args[0];
            port = Integer.parseInt(args[1]);
        }
        Client client = new Client(host, port);
        try {
            client.start();
        } catch (Exception e) {
            System.err.println("client error");
            e.printStackTrace();
        } finally {
            client.close();
        }
    }

}
