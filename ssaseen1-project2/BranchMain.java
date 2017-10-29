import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class BranchMain {

	static public BranchHandler handler = null;
	static public Branch.Processor<Branch.Iface> processor = null;
	static String branchName = null; 
	static int port = 0;
	static Random random = new Random();
	static public volatile ConcurrentHashMap<String, Integer> msgID = new ConcurrentHashMap<>();
	private static final Semaphore _mutex = new Semaphore(1);
	
	public static void main(String args[]) throws TTransportException, InterruptedException{
		
		handler = new BranchHandler();
		processor = new Branch.Processor<Branch.Iface>(handler);

		if(args.length != 2){
			System.err.println("Enter the branch name and the port number");
			System.exit(1);
		}
		branchName = args[0];
		port = Integer.parseInt(args[1]);
		System.out.println("Branch name: "+branchName+" Port: "+port);
		Thread receive = new Thread(new Runnable() {
			@Override
			public void run() {
				// TODO Auto-generated method stub
				BranchServer();
			}
		});
		
		Thread send = new Thread(new Runnable() {
			@Override
			public void run() {
				// TODO Auto-generated method stub
				while(true){
					int n = (random.nextInt(6) + 0)*1000;
					try {
						Thread.sleep(n);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					BranchClient();
				}
			}
		});
		receive.start();
		send.start();
	}	

	public static void BranchServer() {
		// TODO Auto-generated method stub
		
		try {
			TServerTransport server = null;
			TServer tserver = null ;
		    server = new TServerSocket(port);
			tserver = new TThreadPoolServer(new TThreadPoolServer.Args(server).processor(processor));
			BranchHandler.bname = branchName;
		    tserver.serve();
		    
		} catch (TException e) {
			// TODO: handle exception
			e.printStackTrace();
			System.exit(0);
		}					
	}
	
	public static void BranchClient() {
		// TODO Auto-generated method stub
		TransferMessage msg = new TransferMessage();
		
		if(!BranchHandler.snaplist.isEmpty()) {
			for(int i = 0; 	i < BranchHandler.snaplist.size(); i++ ){
				for(BranchID each_branch: BranchHandler.blist){
				TTransport ct = new TSocket(each_branch.getIp(),each_branch.getPort());
				try {
					ct.open();
				} catch (TTransportException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				TProtocol protocol = new TBinaryProtocol(ct);
				Branch.Client client = new Branch.Client(protocol);
				try {
					try {
						_mutex.acquire();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					int lastUsedMessageID = msgID.get(each_branch.getName());
					lastUsedMessageID++;
					msgID.put(each_branch.getName(), lastUsedMessageID);
					_mutex.release();
					client.Marker(BranchHandler.bid, BranchHandler.snaplist.get(i), lastUsedMessageID);	
					
					
				} catch (TException e) { 
					// TODO Auto-generated catch block
					e.printStackTrace();
				}		
				ct.close();
				}
			}
			BranchHandler.snaplist.clear();
		}
		
		else if (!BranchHandler.blist.isEmpty()){
			msg.orig_branchId = BranchHandler.bid; 
			msg.setOrig_branchIdIsSet(true);
			float r = (random.nextInt(5) + 1);
			int amountsent = (int) (r*BranchHandler.current_balance)/100;
			int in = random.nextInt(BranchHandler.blist.size());
			BranchID recBranch = BranchHandler.blist.get(in); //randomize
			try {
				_mutex.acquire();
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			BranchHandler.current_balance = BranchHandler.current_balance - amountsent;
			int lastUsedMessageID = msgID.get(recBranch.getName());
			lastUsedMessageID++;
			msgID.put(recBranch.getName(), lastUsedMessageID);
				_mutex.release();
			msg.setAmount(amountsent);
			msg.setAmountIsSet(true);	
			
			TTransport ct = new TSocket(recBranch.getIp(),recBranch.getPort());
			try {
				ct.open();
			} catch (TTransportException e) { 
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			TProtocol protocol = new TBinaryProtocol(ct);
			Branch.Client client = new Branch.Client(protocol);
			
			try {
				client.transferMoney(msg, lastUsedMessageID);
				
			} catch (TException e) { 
				// TODO Auto-generated catch block
				e.printStackTrace();
			}		
			ct.close();
		}
	}
}
