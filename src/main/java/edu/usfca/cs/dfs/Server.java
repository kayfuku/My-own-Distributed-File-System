package edu.usfca.cs.dfs;

import java.io.IOException;

import edu.usfca.cs.dfs.net.ServerMessageRouter;

public class Server {

    ServerMessageRouter messageRouter;
	private static int port;
	

    public Server() { }

    public void start() throws IOException {
        messageRouter = new ServerMessageRouter(null, null);
		messageRouter.listen(port);
		System.out.println("Listening for connections on port " + port);
    }

    public static void main(String[] args) throws IOException {
		port = Integer.parseInt(args[0]);
        Server s = new Server();
        s.start();
    }
}
































