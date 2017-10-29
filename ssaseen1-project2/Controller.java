import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class Controller {
	
	static int initialAmount;
	static Random r = new Random();
	static public volatile List<BranchID> branchList = new ArrayList<BranchID>();
	static int flag = 0;
	
	public static void main(String args[]) throws IOException{	
		String branchName = null;
		String branchIp = null;
		int branchPort = 0;
		
		if(args.length != 2){
			System.err.println("Enter the initial amount and txt file name");
			System.exit(1);
		}
		
		initialAmount = Integer.parseInt(args[0]);
		System.out.println("Total Amount in the system: "+initialAmount);
		FileReader filename = new FileReader(args[1]);
		BufferedReader br = new BufferedReader(filename);
		String content = null;
		try{
			while((content = br.readLine())!= null){
				 //System.out.println(content);
				 String token[] = content.split("\\s+");
				 if(token.length == 3){
				 branchName = token[0];
				 branchIp = token[1];
				 branchPort = Integer.parseInt(token[2]);
				 BranchID branchId = new BranchID();		 
				 branchId.setName(branchName);
				 branchId.isSetName();
				 branchId.setIp(branchIp);
				 branchId.isSetIp();
				 branchId.setPort(branchPort);
				 branchId.isSetPort();
				 branchList.add(branchId);
				 }
			}
			if(branchList.size() == 0){
				System.err.println(" Enter valid branches in the input file");
				System.exit(1);
			}
			
			int list_size = branchList.size();
			int a ;
			Float amountPerBranch = new Float(initialAmount / list_size);
			if((initialAmount % list_size) > 0){
				flag =1;
			}
			
			for (BranchID each_branch: branchList){
				try {
					TTransport transport = null;
					transport = new TSocket(each_branch.getIp(),each_branch.getPort());
					transport.open();
					TProtocol protocol = new TBinaryProtocol(transport);
					Branch.Client client = new Branch.Client(protocol);	
					try {
						if(flag == 1) {
							a = amountPerBranch.intValue()+1;
							flag = 0;
						}
						else 
							a = amountPerBranch.intValue();
							client.initBranch(a, branchList);
					} catch (TException e) {
						e.printStackTrace();
						System.exit(1);
					}
					transport.close();
				} catch (TException e) {
					// TODO: handle exception
					e.printStackTrace();
				}		
			}
			
			Thread initSnapShot = new Thread(new Runnable() {
				@Override
				public void run() {
					// TODO Auto-generated method stub
					int snapshotId = 1;
					while(true){	
					try {
						Thread.sleep(6000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					try {
						initSnapShot(branchList,snapshotId);
					} catch (TTransportException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					snapshotId = snapshotId + 1;	
						
					}	
				}
			});
			
			Thread retreiveSnapShot = new Thread(new Runnable(){

				@Override
				public void run() {
					int snapshotId = 1;
					// TODO Auto-generated method stub
					while(true){
						try {
							Thread.sleep(20000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						try {
							retreiveSnapShot(branchList,snapshotId);
						} catch (TTransportException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						snapshotId = snapshotId + 1;
					}
				}
				
			});
			
			initSnapShot.start();
			retreiveSnapShot.start();
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
		
	
	private static void retreiveSnapShot(List<BranchID> branchList2, int snapshotId2) throws TTransportException {
		// TODO Auto-generated method stub	
		System.out.println("============================================");
		System.out.println("SNAPSHOT NUMBER: "+snapshotId2);
		for(BranchID each_branch: branchList){
			TTransport transport = null;
			transport = new TSocket(each_branch.getIp(),each_branch.getPort());
			transport.open();
			TProtocol protocol = new TBinaryProtocol(transport);
			Branch.Client client = new Branch.Client(protocol);
			try {
				LocalSnapshot ls = client.retrieveSnapshot(snapshotId2);
				System.out.println(each_branch.getName()+": "+ls.getBalance());
				int k=0;
				for(int j=0; j<branchList.size(); j++) {
					if(branchList.get(j).getName().equals(each_branch.getName())) {
						
					}
					else {
						System.out.println(branchList.get(j).getName()+"-->"+each_branch.getName()+": "+ls.getMessages().get(k));
						k++;
					}
				}
				System.out.println("============================================");
			} catch (SystemException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			transport.close();
		}
	}

	private static void initSnapShot(List<BranchID> branchList2, int snapshotId) throws TTransportException {
		// TODO Auto-generated method stub
		int i = r.nextInt(branchList2.size());
		BranchID bid = branchList.get(i); //randomize!!
		TTransport transport = null;
		transport = new TSocket(bid.getIp(),bid.getPort());
		transport.open();
		TProtocol protocol = new TBinaryProtocol(transport);
		Branch.Client client = new Branch.Client(protocol);
		try {
			client.initSnapshot(snapshotId);
		} catch (SystemException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		transport.close();
	}
}