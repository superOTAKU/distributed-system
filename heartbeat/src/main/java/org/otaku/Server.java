package org.otaku;

import java.io.DataInputStream;
import java.io.InputStream;
import java.net.*;
import java.nio.charset.StandardCharsets;

/**
 * 实现一个简单的Echo Server
 */
public class Server {
    private final int port;

    public Server(int port) {
        this.port = port;
    }

    private static class ClientHandlerThread extends Thread {
        private final Socket socket;

        ClientHandlerThread(Socket socket) {
            this.socket = socket;
            setDaemon(true);
            setName("client-" + socket);
        }

        @Override
        public void run() {
            System.out.println("client-" + socket + " connected");
            try {
                while (true) {
                    //使用定长协议
                    InputStream is = socket.getInputStream();
                    DataInputStream dis = new DataInputStream(is);
                    int len = dis.readInt();
                    byte[] data = new byte[len];
                    is.readNBytes(data, 0, len);
                    String dataStr = new String(data, StandardCharsets.UTF_8);
                    if (dataStr.equals("HEARTBEAT")) {
                        System.out.println("client-" + socket + " receive heartbeat");
                    } else {
                        System.out.println("client-" + socket + " say: " + dataStr);
                    }
                }
            } catch (SocketTimeoutException timeoutException) {
                System.err.println("client-" + socket + " timeout");
            }catch (Exception e) {
                System.err.println("client-" + socket + " process error");
                e.printStackTrace();
            } finally {
                try {
                    socket.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public void start() throws Exception {
        ServerSocket serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress("0.0.0.0", port), 4096);
        System.out.println("server listen at " + port);
        while (true) {
            Socket client = serverSocket.accept();
            //快速发送小包
            client.setOption(StandardSocketOptions.TCP_NODELAY, true);
            //设置了超时时间，很关键，只要发生了超时，我们将关闭客户端
            client.setSoTimeout(5000);
            new ClientHandlerThread(client).start();
        }
    }


    //程序入口
    public static void main(String[] args) {
        int port;
        if (args.length == 0) {
            port = 80;
        } else {
            port = Integer.parseInt(args[0]);
        }
        Server server = new Server(port);
        try {
            server.start();
        } catch (Exception e) {
            System.err.println("server exception occur");
            e.printStackTrace();
        }
    }
}
