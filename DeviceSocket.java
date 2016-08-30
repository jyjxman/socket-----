package com.zbiti.odn.socket;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.BindException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



import org.apache.log4j.Logger;

import com.zbiti.odn.bean.ReceiveQueue;
import com.zbiti.odn.bean.SendQueue;
import com.zbiti.odn.message.MessageManager;
import com.zbiti.odn.service.SocketServiceWrap;
import com.zbiti.odn.util.BtData;
import com.zbiti.odn.util.ByteHelper;
import com.zbiti.odn.util.GetBtOrderCmdUtil;

/**
 * @author 王元晓
 * 
 * 
 */
public class DeviceSocket {
	private volatile boolean exit;//线程是否退出
	//private static boolean insert = false;//socket异常

	private static Logger logger = Logger.getLogger(DeviceSocket.class);

	private static SocketServiceWrap socketServiceWrap = null;

	//每个设备对应一个报文系列号 每次通信+1	
	public static byte[] Ser = { 0x00, 0x01 };
	private static StringBuffer responseBuffer = new StringBuffer();//响应消息存放区
	private static final String s_flag = "7E0010";//开始标志
	private static final String e_flag = "7E";//结束标志

	private static String ip; // 设备IP地址，即SERVER端的IP地址
	private int port; // 设备开放的端口，即server端开放的端口

	//	private ConcurrentLinkedQueue<BtData> btDataQueue; // 指令队列，线程安全的，非阻塞的
	/** 1、创建执行命令返回数据的队列，static final 修饰类初始化时创建 */
	//	private ConcurrentLinkedQueue<RecieveData> recieveDataQueue;

	//存在公共传递参数 ：资源收集存放 
	private static Map<String, Object> paramsMap;

	private Socket socket = null;
	private InputStream in = null;
	private OutputStream out = null;

	/**
	 * 构造方法： 根据IP地址、端口号跟socket server建立连接，同时启动接收、发送两个线程，打通收、发通道
	 * 
	 * @throws InterruptedException
	 */
	public DeviceSocket(String ipAddr, int port, SocketServiceWrap socketServiceWrap) {
		exit = false;//线程运行状态

		this.ip = ipAddr;
		this.port = port;
		this.socketServiceWrap = socketServiceWrap;
		paramsMap = new HashMap<String, Object>();

		//btDataQueue = new ConcurrentLinkedQueue<BtData>();
		//recieveDataQueue = new ConcurrentLinkedQueue<RecieveData>();

		//sleep 100  让子线程能够正常启动
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		new SendSocketClient().start();
		new RecieveSocketClient().start();

		//new CheckSocketStatus().start();//定时检查的工作放在发送和接收之前做判断，减少了定时检查线程的消耗，但是增加了clinet 和  service之间的通信
	}

	/**
	 * 往指令队列中添加多条指令
	 */
	public void putBtDataGroup(List<BtData> btDataGroup) {
		for (BtData btData : btDataGroup) {
			//btDataQueue.add(btData);
		}
	}

	/**
	 * 往指令队列中添加一条指令
	 */
	public void putBtDataGroup(BtData btData) {
		//logger.info("--------------添加一条指令到队列--------->" + ByteHelper.BytesToHexString(btData.composite()));
		//btDataQueue.add(btData);

	}

	/**
	 * 发送的内部类： 发送线程，该线程只负责发送
	 */
	class SendSocketClient extends Thread {

		public void run() {
			logger.info("-------------发送线程线程启动成功！-------------");
			while (!exit) {
				logger.info("发送线程while正在执行....");
				try {
					this.send();
				} catch (UnknownHostException e) {
					exit = true;
					logger.error("send UnknownHostException 发送线程停止");
					logger.error(DeviceSocket.class.getSimpleName(), e);

					logger.info("socket连接异常，正在入库....");
					socketServiceWrap.getPhyEquipmentDao().addSocketGjSb(ip, "", "", "", "通信中断(SOCKET)", new Date());
					logger.info("socket连接异常，入库成功！");

					//移除deviceSocket
					try {
						closeAll();
					} catch (IOException e1) {
						logger.error("socket clsoe");
					}
					DeviceSocketFactory.removeDeviceSocketByIp(ip, port);
					SocketClientPool.getSocketPool().removeSocketClient(ip, port);

				} catch (BindException e) {
					logger.info("send Address already in use: JVM_Bind");
					//TO DO
					//移除deviceSocket
//					SocketClientPool.getSocketPool().removeSocketClient(ip, port);
//					DeviceSocketFactory.removeDeviceSocketByIp(ip, port);
//					try {
//						closeAll();
//					} catch (IOException e1) {
//						logger.error("socket clsoe");
//					}
				} catch (IOException e) {
					exit = true;
					logger.error("send IOException 发送线程停止");
					logger.error(DeviceSocket.class.getSimpleName(), e);

					//logger.info("socket连接异常，正在入库....");
					//socketServiceWrap.getPhyEquipmentDao().addSocketGjSb(ip, "", "", "", "通信中断(SOCKET)", new Date());
					//logger.info("socket连接异常，入库成功！");

					//移除deviceSocket
					try {
						closeAll();
					} catch (IOException e1) {
						logger.error("socket clsoe");
					}
					DeviceSocketFactory.removeDeviceSocketByIp(ip, port);
					SocketClientPool.getSocketPool().removeSocketClient(ip, port);
				} catch (InterruptedException e) {
					logger.error("send InterruptedException 发送线程停止");
					logger.error(DeviceSocket.class.getSimpleName(), e);
				}
			}
		}

		/**
		 * 逐条发送指令队列中的指令
		 * 
		 * @throws IOException
		 * @throws UnknownHostException
		 * @throws InterruptedException
		 * 
		 * @throws SQLException
		 */
		public void send() throws UnknownHostException, IOException, InterruptedException {
			socket = SocketClientPool.getSocketPool().getSocketClient(ip, port);
			SendQueue sendQueueParams = new SendQueue();
			sendQueueParams.setIp(ip);
			sendQueueParams.setServerPort(port);
			executeSendQueue(socket, sendQueueParams);
		}

		/***
		 * 处理发送队列
		 * 
		 * @throws InterruptedException
		 * @throws IOException
		 */
		private void executeSendQueue(Socket socket, SendQueue sendQueueParams) throws InterruptedException, IOException {
			//判断socket连接是否正常
			isConnected(socket);
			SendQueue sendQueue = socketServiceWrap.getQueueService().getSendQueue(sendQueueParams);
			if (null != sendQueue) {
				int timeOut = sendQueue.getTimeOut();
				int status = sendQueue.getStatus();
				int executeCnt = sendQueue.getExecuteCnt();
				long id = sendQueue.getId();
				String cmdContent = sendQueue.getCmdContent();
				long maxLayTime = sendQueue.getMaxLayTime();
				/** 1、status:0--未开始，1--进行中，2--完成 */
				SendQueue sendQueueOBj = new SendQueue();
				sendQueueOBj.setId(id);
				byte[] cmdcontent_byte = null;
				BtData btData = null;
				if (status == 0) {
					/** 2、修改请求队列状态字段 */
					sendQueueOBj.setStatus(1);
					sendQueueOBj.setExecuteCnt(1);
					int count = socketServiceWrap.getQueueService().updateSendQueue(sendQueueOBj);
					/** 3、将String转成BtData发送 */
					//					BtData btData = new BtData(ByteHelper.HexStringToBytes(cmdContent));
					List<String> cmdList = GetBtOrderCmdUtil.getPatternList(s_flag, e_flag, cmdContent);
					/** 4、如果正则表达式不存在就删除这条指令，不发送，否则执行 */
					if (cmdList != null && cmdList.size() > 0) {
						//将7E5D 7D5E 转义
						cmdcontent_byte = ByteHelper.Bytes7D5D(ByteHelper.Bytes7D5E(ByteHelper.HexStringToBytes(cmdContent)));
						btData = BtData.HStringOrderToBtData(cmdcontent_byte);
						sendBtData(socket, btData, sendQueueOBj);
					} else {
						int effect = socketServiceWrap.getQueueService().addHisSendQueue(sendQueueOBj);
						int effectNumber = socketServiceWrap.getQueueService().delSendQueue(sendQueueOBj);
					}
				} else if (status == 1) {
					/**
					 * 5、timeOut：1、超时
					 * 2、未超时，如果超时并且执行次数<3，将执行次数+1,重复的机制,如果重发次数大于3次删除记录
					 */
					if (timeOut == 1 && executeCnt < 4) {
						sendQueueOBj.setExecuteCnt(executeCnt + 1);
						int count = socketServiceWrap.getQueueService().updateSendQueue(sendQueueOBj);
						cmdcontent_byte = ByteHelper.Bytes7D5D(ByteHelper.Bytes7D5E(ByteHelper.HexStringToBytes(cmdContent)));
						btData = BtData.HStringOrderToBtData(cmdcontent_byte);

						sendBtData(socket, btData, sendQueueOBj);
					} else if (timeOut == 1 && executeCnt >= 4) {
						int count = socketServiceWrap.getQueueService().addHisSendQueue(sendQueueOBj);
						int cnt = socketServiceWrap.getQueueService().delSendQueue(sendQueueOBj);
					}
				} else if (status == 2) {
					int count = socketServiceWrap.getQueueService().addHisSendQueue(sendQueueOBj);
					int cnt = socketServiceWrap.getQueueService().delSendQueue(sendQueueOBj);
				}
				this.sleep(40);
			}

		}

		/**
		 * 发送单条指令
		 * 
		 * @throws IOException
		 */
		private void sendBtData(Socket socket, BtData btData, SendQueue sendQueueObj) throws IOException {
			sendQueueObj.setStartTime(new Date());
			int count = socketServiceWrap.getQueueService().updateSendQueue(sendQueueObj);

//			if (isConnected(socket)) {
				OutputStream out = socket.getOutputStream(); // 获取socket的输出流

				logger.info("----------------发送指令------------>" + ByteHelper.BytesToHexString(btData.composite()));
				//指令入库
				//insertOrder(ByteHelper.BytesToHexString(btData.composite()), 1, btData.getTimeOut());

				out.write(btData.composite(), 0, btData.composite().length); // 将字节数组转换成二进制的数组发送给硬件设备（即server端）
				out.flush();
//			}

		}

		/**
		 * 发送单条指令
		 */
		private boolean sendBtData(Socket socket, BtData btData) {
			try {
				out = socket.getOutputStream(); // 获取socket的输出流

				logger.info("----------------发送指令------------>" + ByteHelper.BytesToHexString(btData.composite()));
				//指令入库
				//insertOrder(ByteHelper.BytesToHexString(btData.composite()), 1, btData.getTimeOut());

				out.write(btData.composite(), 0, btData.composite().length); // 将字节数组转换成二进制的数组发送给硬件设备（即server端）
				out.flush();
			} catch (Exception e) {
				logger.error(DeviceSocket.class.getSimpleName(), e);
				return false;
			}
			return true;
		}

	}

	/**
	 * 接收的内部类： 接收线程，该线程只负责接收，被动的不断接收
	 */
	class RecieveSocketClient extends Thread {
		public void run() {
			logger.info("-------------接收响应线程启动成功！-------------");
			while (!exit) {
				logger.info("接收线程while正在执行....");

				try {
					this.recieve();
				} catch (UnknownHostException e) {
					exit = true;
					logger.error("recieve UnknownHostException 接收线程停止");
					logger.error(DeviceSocket.class.getSimpleName(), e);

					logger.info("socket连接异常，正在入库....");
					socketServiceWrap.getPhyEquipmentDao().addSocketGjSb(ip, "", "", "", "通信中断(SOCKET)", new Date());
					logger.info("socket连接异常，入库成功！");

					//移除deviceSocket
					try {
						closeAll();
					} catch (IOException e1) {
						logger.error("socket clsoe");
					}
					DeviceSocketFactory.removeDeviceSocketByIp(ip, port);
					SocketClientPool.getSocketPool().removeSocketClient(ip, port);
				} catch (BindException e) {
					logger.info("recieve Address already in use: JVM_Bind");

					//移除deviceSocket
//					SocketClientPool.getSocketPool().removeSocketClient(ip, port);
//					DeviceSocketFactory.removeDeviceSocketByIp(ip, port);
//					try {
//						closeAll();
//					} catch (IOException e1) {
//						logger.error("socket clsoe");
//					}
				} catch (IOException e) {
					//接收线程对io异常不做处理，如果处理会导致发送，接收同时接收到io异常，处理两次的情况
					exit = true;
					logger.error("recieve IOException 接收线程停止");
					logger.error(DeviceSocket.class.getSimpleName(), e);

					logger.info("socket连接异常，正在入库....");
					socketServiceWrap.getPhyEquipmentDao().addSocketGjSb(ip, "", "", "", "通信中断(SOCKET)", new Date());
					logger.info("socket连接异常，入库成功！");

					//移除deviceSocket
					try {
						closeAll();
					} catch (IOException e1) {
						logger.error("socket clsoe");
					}
					DeviceSocketFactory.removeDeviceSocketByIp(ip, port);
					SocketClientPool.getSocketPool().removeSocketClient(ip, port);
				} catch (InterruptedException e) {
					logger.error("recieve InterruptedException 接收线程停止");
					logger.error(DeviceSocket.class.getSimpleName(), e);
				}

			}
		}

		/**
		 * 不断接收响应，每接收到一条响应，就发送给所有的监听者，由监听者去处理
		 * 
		 * @throws UnknownHostException
		 * 
		 * @throws IOException
		 * @throws InterruptedException
		 */
		public void recieve() throws UnknownHostException, IOException, InterruptedException {
			socket = SocketClientPool.getSocketPool().getSocketClient(ip, port);
//			if (isConnected(socket)) {
				//将每次读取到的字节以HString格式拼接起来,去截取
				byte[] readByte = this.recieveBtData(socket);

				//读取到字节
				if (null != readByte && readByte.length > 0) {

					DeviceSocketFactory.getDeviceSocketByIp(ip, port, socketServiceWrap).responseBuffer.append(ByteHelper.BytesToHexString(readByte));

					//当前的响应数据
					String currentResponse = DeviceSocketFactory.getDeviceSocketByIp(ip, port, socketServiceWrap).responseBuffer.toString();

					List<String> responseList = GetBtOrderCmdUtil.getPatternList(s_flag, e_flag, currentResponse);

					//flag 响应解析是否正确
					boolean flag = true;
					int end = 0;
					for (String string : responseList) {
						List<String> cmdList = GetBtOrderCmdUtil.getPatternList(s_flag, e_flag, string);
						if (null != cmdList && cmdList.size() > 0) {
							end += string.length();
						} else {
							flag = false;
							break;
						}

					}

					if (flag) {
						DeviceSocketFactory.getDeviceSocketByIp(ip, port, socketServiceWrap).responseBuffer.delete(0, end);

						//截取7E0100.....7E的格式
						//List<String> responseList = cutOrder(currentResponse, s_flag, e_flag, new ArrayList<String>());

						//将响应入库
						if (null != responseList && responseList.size() > 0) {
							for (String cmd_content : responseList) {
								//根据响应String构造BtData

								//将7E5D 7D7D 转义
								byte[] cmdcontent_byte = ByteHelper.Bytes7D5D(ByteHelper.Bytes7D5E(ByteHelper.HexStringToBytes(cmd_content)));
								BtData btdata = BtData.HStringOrderToBtData(cmdcontent_byte);

								String serialNo = ByteHelper.BytesToHexString(ByteHelper.ReverseBytes(btdata.getSer()));
								String cmdNo = ByteHelper.BytesToHexString(ByteHelper.ReverseBytes(btdata.getOrder()));
								String cmdContent = ByteHelper.BytesToHexString(btdata.composite());

								ReceiveQueue receiveQueue = new ReceiveQueue();
								receiveQueue.setIp(ip);
								receiveQueue.setServerPort(port);
								receiveQueue.setCmdNo(cmdNo);
								receiveQueue.setCmdContent(cmdContent);
								receiveQueue.setSerialNo(serialNo);
								/** 1、添加接收队列数据 */
								int cnt = socketServiceWrap.getQueueService().addReceiveQueue(receiveQueue);
								SendQueue sendQueue = new SendQueue();
								sendQueue.setIp(ip);
								sendQueue.setServerPort(port);
								sendQueue.setCmdNo(cmdNo);
								sendQueue.setSerialNo(serialNo);
								sendQueue.setStatus(2);
								/** 2、修改发送队列为成功状态 */
								int count = socketServiceWrap.getQueueService().updateSendQueue(sendQueue);
								/** 3、分配监听者 */
								RecieveData rd = new RecieveData(ip, port, cmdNo, btdata);
								this.adviceListner(rd);

							}
						}
					}
//				}
			}
		}

		/**
		 * 从输入流里接收响应
		 * 
		 * @throws IOException
		 */
		public byte[] recieveBtData(Socket socket) throws IOException {
			byte[] buffer = new byte[1024];
			int len;
			//接收前进行确认连接是否正常
//			isConnected(socket);
			in = socket.getInputStream();
			len = in.read(buffer);
			logger.info("------------接收响应-----------长度： " + len + " 内容为： " + ByteHelper.BytesToHexString(Arrays.copyOf(buffer, len)));
			return Arrays.copyOf(buffer, len);
		}

		/** 3、通知监听者 */
		public void adviceListner(RecieveData rd) {
			MessageManager.getMessageManager().recieve(rd); // 通知监听者，根据接收对象做相应的处理
		}
	}

	/**
	 * 定时检查socket连接状态
	 * 
	 * @author 28058
	 * 
	 */
	class CheckSocketStatus extends Thread {
		@Override
		public void run() {
			logger.info("-------------检查socket连接状态线程启动成功！-------------");
			while (true) {
				//定时检查sokcet状态
				try {
					Thread.sleep(5000);
					boolean b = true;
					b = this.checkSocketStatus();
				} catch (Exception e) {
					logger.error(DeviceSocket.class.getSimpleName(), e);
				}

				//if (!b) {
				//	break;
				//}
			}
		}

		/**
		 * @throws IOException
		 * @throws UnknownHostException
		 * 
		 */
		public boolean checkSocketStatus() throws Exception {
			Socket socket = SocketClientPool.getSocketPool().getSocketClient(ip, port);

			boolean b = isConnected(socket);
			if (!b) {
				//socket实例去掉
				SocketClientPool.getSocketPool().removeSocketClient(ip, port);

				logger.info("socket连接异常，正在入库....");
				socketServiceWrap.getPhyEquipmentDao().addSocketGjSb(ip, "", "", "", "通信中断(SOCKET)", new Date());
				logger.info("socket连接异常，入库成功！");
				return false;
			}
			return true;
		}

	}

	/**
	 * 判断socket是否断开
	 * 
	 * @param s
	 * @return
	 * @throws IOException
	 */
	public static boolean isConnected(Socket s) throws IOException {
		s.sendUrgentData(0xFF);
		return true;
	}

	//	/**
	//	 * 判断socket是否断开
	//	 * 
	//	 * @param s
	//	 * @return
	//	 */
	//	public static boolean isConnected(Socket s) {
	//		try {
	//			s.sendUrgentData(0xFF);
	//			insert = false;
	//			return true;
	//		} catch (Exception e) {
	//			exit = true;//停止线程
	//			logger.error(DeviceSocket.class.getSimpleName(), e);
	//			//TO DO
	//			if (!insert) {
	//				logger.info("socket连接异常，正在入库....");
	//				socketServiceWrap.getPhyEquipmentDao().addSocketGjSb(ip, "", "", "", "通信中断(SOCKET)", new Date());
	//				logger.info("socket连接异常，入库成功！");
	//			}
	//			insert = true;
	//
	//			return false;
	//		}
	//		//		finally {
	//		//			//通信异常
	//		//			try {
	//		//				if (socket != null) {
	//		//					socket.close();
	//		//				}
	//		//			} catch (IOException e) {
	//		//				logger.error("socket close");
	//		//				logger.error(DeviceSocket.class.getSimpleName(), e);
	//		//			}
	//		//		}
	//	}

	/**
	 * @Description: 截取完整响应
	 * @param @param str
	 * @param @param s_flag
	 * @param @param e_flag
	 * @param @param list
	 * @param @return
	 * @return List<String>
	 * @throws
	 * @author jyj
	 */
	public static List<String> cutOrder(String str, String s_flag, String e_flag, List list) {
		logger.info("当前Buffer中的字符串 ：" + responseBuffer.toString());
		if (str.indexOf(s_flag) == -1) {
			return null;
		} else if (str.indexOf(s_flag) != -1) {
			int s_index = str.indexOf(s_flag);
			String endString = str.substring(s_index + s_flag.length());
			int e_index = endString.indexOf(e_flag);

			if (e_index != -1) {
				//已经截取一条
				String order = str.substring(s_index, e_index + s_flag.length() + e_flag.length());
				logger.info("order: " + order);
				list.add(order);

				//处理响应数据
				responseBuffer.delete(s_index, e_index + s_flag.length() + e_flag.length());
				logger.info("剩余response ：" + responseBuffer.toString());
				//遍历
				cutOrder(responseBuffer.toString(), s_flag, e_flag, list);
			}
			return list;
		}
		return list;
	}

	public static Map<String, Object> getParamsMap() {
		return paramsMap;
	}

	public static void setParamsMap(Map<String, Object> paramsMap) {
		DeviceSocket.paramsMap = paramsMap;
	}

	/**
	 * 
	 * @Description: 关闭输入、输出、socket
	 * @param @throws IOException
	 * @return void
	 * @throws
	 * @author jyj
	 */
	private void closeAll() throws IOException {
		if (null != out) {
			logger.info("wo yao  shanchu  out");
			out.close();
		}
		if (null != in) {
			logger.info("wo yao  shanchu  in");
			in.close();
		}
		if (null != socket) {
			logger.info("wo yao  shanchu  socket");
			socket.close();
		}

	}

}
