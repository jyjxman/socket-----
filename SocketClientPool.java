package com.zbiti.odn.socket;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;

import javax.annotation.Resource;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import com.zbiti.odn.service.SocketServiceWrap;
import com.zbiti.odn.util.ReadJDBCPropertiesUtil;

/*单例模型的socket client的池*/
@Service
public class SocketClientPool {

	private Logger logger = Logger.getLogger(SocketClientPool.class);

	@Resource
	private SocketServiceWrap socketServiceWrap;

	private static SocketClientPool socketClientPool = new SocketClientPool();

	/* socket 客户端的集合，用来存储所有的socket，为了应标临时使用 */
	private static HashMap<String, Socket> socketClients = new HashMap<String, Socket>();

	private SocketClientPool() {
	}

	public static SocketClientPool getSocketPool() {
		return socketClientPool;
	}

	public void reconnectSocket(String ipAddr, int port) {
		try {
			Socket socketClient = socketClients.get(ipAddr + "_" + port);
			if (socketClient != null) {
				socketClient.close();
				this.getSocketClient(ipAddr, port);
			}
		} catch (Exception e) {
		}
	}

	/* 获取socket 客户端，如果该IP地址已经存在就直接返回socket client，如果不存在则先新建 */
	public Socket getSocketClient(String ipAddr, int port) throws UnknownHostException, IOException {
		Socket socketClient = socketClients.get(ipAddr + "_" + port);
		if (socketClient == null) {
			//try {
			//socketClient = new Socket(ipAddr, port);

			//TO DO 本地端口需要在主控时指定
			//socketClient = new Socket(ipAddr, port,InetAddress.getLocalHost(), 33333);

			socketClient = new Socket(); //此时Socket对象未绑定到本地端口，并且未连接远程服务器
			socketClient.setReuseAddress(true);
			socketClient.setSoLinger(true, 0);//调用close立即释放资源（包括端口）
			//socketClient.setOOBInline(true);//表示支持发送一个字节的 TCP 紧急数据
			SocketAddress localAddr = new InetSocketAddress(InetAddress.getLocalHost(), 33333);
			SocketAddress remoteAddr = new InetSocketAddress(ipAddr, port);
			socketClient.bind(localAddr); //与本地端口绑定
			socketClient.connect(remoteAddr); //连接远程服务器，并且绑定匿名的本地端口

			socketClients.put(ipAddr + "_" + port, socketClient);
			logger.info("新建socket连接： ip：" + ipAddr + "  port: " + port + " 本地端口号： 33333" + "已经保存在SocketClient中.");

		}
		return socketClient;
	}

	/* 根据ip port 删除socket 客户端 */
	public void removeSocketClient(String ipAddr, int port) {
		Socket socketClient = socketClients.get(ipAddr + "_" + port);
		if (socketClient != null) {
			logger.info("移除socket连接： ip：" + ipAddr + "  port: " + port);
			socketClients.remove(ipAddr + "_" + port);
			logger.info("移除socket连接： ip：" + ipAddr + "  port: " + port + ",已经被移除！");
		}

	}

	public void closeSocketClient() {
		// 目前什么都不做，招标结束后需要修改。client不关闭，告警才可以继续
		logger.info("client is closed");

	}

}
