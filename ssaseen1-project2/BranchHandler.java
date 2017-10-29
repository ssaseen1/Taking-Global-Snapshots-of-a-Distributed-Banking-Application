import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.thrift.TException;

public class BranchHandler implements Branch.Iface{
	
	public static volatile int current_balance = 0;
	public static String bname = null;
	public static volatile List<BranchID> blist = new ArrayList<BranchID>();
	public static volatile List<Integer> snaplist = new CopyOnWriteArrayList();
	public static volatile Map<Integer, LocalSnapshot> snapshotStatus = new HashMap<>();
	public static volatile ConcurrentHashMap<Integer, Map<BranchID, Integer>> localmap = new ConcurrentHashMap<>();
	static volatile BranchID bid = new BranchID();
	public static volatile ConcurrentHashMap<String, Integer> seenMsgID = new ConcurrentHashMap<>();
	public static volatile ConcurrentHashMap<String, Integer> channel = new ConcurrentHashMap<>();
	
	final Lock lock = new ReentrantLock();
	final Condition MsgId = lock.newCondition();

	@Override
	public void initBranch(int balance, List<BranchID> all_branches) throws SystemException, TException {
		// TODO Auto-generated method stub
		int index = 0;
		BranchHandler.current_balance = balance;
		System.out.println("Initial Balance: "+balance);
		
		for(BranchID each_branch: all_branches){
			if(each_branch.getName().equals(bname)){
				bid.setName(bname);
				bid.isSetName();
				bid.setIp(each_branch.getIp());
				bid.isSetIp();
				bid.setPort(each_branch.getPort());
			}
			else{
				blist.add(each_branch);
				BranchMain.msgID.put(each_branch.getName(), 0);
				seenMsgID.put(each_branch.getName(), 0);
				channel.put(each_branch.getName(),index);
				index++;
			}
		}
	}

	@Override
	public void transferMoney(TransferMessage message, int messageId) throws SystemException, TException {
		// TODO Auto-generated method stub
		lock.lock();
		try{
			int lastSeenMsgId = seenMsgID.get(message.getOrig_branchId().getName());
			while(messageId != lastSeenMsgId+1){
				try {
					MsgId.await();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(message.isSetAmount()){
				BranchHandler.current_balance = BranchHandler.current_balance + message.getAmount();
			
				for (Map.Entry<Integer, Map<BranchID, Integer>> entry: localmap.entrySet()){
					Map<BranchID, Integer> entries = entry.getValue();
					if(entries.get(message.getOrig_branchId()) == 1) {
						LocalSnapshot ls = snapshotStatus.get(entry.getKey());
						int index = channel.get(message.getOrig_branchId().getName());
						int value = ls.getMessages().get(index) + message.getAmount();
						List<Integer> updatedList = ls.getMessages();
						updatedList.add(index, value);
						ls.setMessages(updatedList);
						snapshotStatus.put(entry.getKey(), ls);
					}
				}
			}
		seenMsgID.put(message.orig_branchId.getName(), messageId);
		MsgId.signalAll();
		}
		finally{
			lock.unlock();
		}
		
	}

	@Override
	public synchronized void initSnapshot(int snapshotId) throws SystemException, TException {
		// TODO Auto-generated method stub
		LocalSnapshot localsnapshot = new LocalSnapshot();
		localsnapshot.setBalance(current_balance);
		localsnapshot.setBalanceIsSet(true);
		localsnapshot.setSnapshotId(snapshotId);
		localsnapshot.setSnapshotIdIsSet(true);
		List<Integer> list = new ArrayList<>();
		for(int i=0; i<blist.size(); i++) {
			list.add(0);
		}
		localsnapshot.messages = list;
		snapshotStatus.put(snapshotId, localsnapshot);
		
		Map<BranchID, Integer> innermap = new HashMap<>();
		for(BranchID each_branch: blist){
			innermap.put(each_branch, 1);
		}
		localmap.put(snapshotId,innermap);
		snaplist.add(snapshotId);
	}

	@Override
	public void Marker(BranchID branchId, int snapshotId, int messageId) throws SystemException, TException {
		// TODO Auto-generated method stub
		lock.lock();
		try {
			int lastSeenMsgId = seenMsgID.get(branchId.getName());
			while(messageId != lastSeenMsgId+1){
				try {
					MsgId.await();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(!snapshotStatus.containsKey(snapshotId)){
				
				LocalSnapshot localsnapshot = new LocalSnapshot();
				localsnapshot.setBalance(current_balance);
				localsnapshot.setBalanceIsSet(true);
				localsnapshot.setSnapshotId(snapshotId);
				localsnapshot.setSnapshotIdIsSet(true);
				List<Integer> list = new ArrayList<>();
				for(int i=0; i<blist.size(); i++) {
					list.add(0);
				}
				localsnapshot.messages = list;
				snapshotStatus.put(snapshotId, localsnapshot);
				Map<BranchID, Integer> innermap = new HashMap<>();
				for(BranchID each_branch: blist){
					if(each_branch.getName().equals(branchId.getName())){
						innermap.put(each_branch, 0);
					}
					else
				 		innermap.put(each_branch, 1);
				}
				localmap.put(snapshotId,innermap);
				snaplist.add(snapshotId);
			}
			else {
				Map<BranchID, Integer> innermap = localmap.get(snapshotId);
				innermap.put(branchId, 0);
				localmap.put(snapshotId, innermap);		
			}
			seenMsgID.put(branchId.getName(),messageId);
		} finally {
			// TODO: handle finally clause
			lock.unlock();
		}
		
	}

	@Override
	public synchronized LocalSnapshot retrieveSnapshot(int snapshotId) throws SystemException, TException {
		// TODO Auto-generated method stub
		LocalSnapshot retrieveSnapshot = snapshotStatus.get(snapshotId);
		return retrieveSnapshot;
	}
}