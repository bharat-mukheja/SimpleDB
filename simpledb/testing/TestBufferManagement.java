package simpledb.testing;

import java.util.HashMap;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;

public class TestBufferManagement {
	public static void showBufferAllotments(Buffer[] buffers){
		System.out.println("-------Current Buffer Allotments-------");
		System.out.println("Buffer\t\tBlock Number\t\tLSN");
		for (int i = 0; i < buffers.length; i++) {
			System.out.println("\t"+(i+1)+"\t\t"+buffers[i].block().number()+"\t\t"+buffers[i].getLogSequenceNumber());
		}
	}
	
	public static void testBufferReplacementPolicy() {
		// TODO Auto-generated method stub
		BufferMgr bufferMgr = new BufferMgr(8);
		
		System.out.println("---------TEST-1----------");
		System.out.println("1. Create a list of 12 files-blocks.");
		Block[] blks = new Block[12];
		for (int i = 0; i < blks.length; i++) {
			blks[i] = new Block("file-block", i+1);
		}
		System.out.println("Created File Blocks.");
		
		System.out.println();
		System.out.println("---------TEST-2----------");
		System.out.println("2. Check the number of available buffers initially. All should be available as none of them have been pinned yet.");
		System.out.println("Buffers Available="+bufferMgr.available());
		
		System.out.println();
		System.out.println("---------TEST-3----------");
		Buffer[] buffers = new Buffer[8];
		System.out.println("3. Keep pinning buffers one by one and check the number of available buffers.");
		for (int i = 0; i < buffers.length; i++) {
			buffers[i] = bufferMgr.pin(blks[i]);
			System.out.println("Available buffers : " + bufferMgr.available() + "\n");
		}
		showBufferAllotments(buffers);
		System.out.println();
		System.out.println("---------TEST-4----------");
		System.out.println("4. When all buffers have been pinned, if pin request is made again, throw an exception");
		try {
			// Should throw exception
			System.out.println("No available buffers. Waiting for 10 seconds too check if a buffer becomes available.");
			Buffer buff = bufferMgr.pin(blks[8]);
			System.out.println("Unexpected behavior : Exception Expected.");
		} catch (simpledb.buffer.BufferAbortException e) {
			// Expected behavior.
			System.out.println("****** Buffer Abort Exception thrown - TimedOut ******");
		} catch (Exception e) {
			e.printStackTrace();
		}
		showBufferAllotments(buffers);
		System.out.println();
		System.out.println("---------TEST-5----------");
		System.out.println("5. Unpin a few buffers and see if you are still getting an exception or not.");
		try {
			System.out.println("Unpinned buffer 7");
			bufferMgr.unpin(buffers[6]);
			showBufferAllotments(buffers);
			System.out.println("\nUnpinned buffer 6");
			bufferMgr.unpin(buffers[5]);
			showBufferAllotments(buffers);
			System.out.println("Pinning again");
			Buffer buff = bufferMgr.pin(blks[8]);
			System.out.println("Successfully pinned a buffer after unpinning couple of buffers");
			showBufferAllotments(buffers);
		} catch (simpledb.buffer.BufferAbortException e) {
			// Unexpected behavior.
			System.out.println("****** Buffer Abort Exception thrown - TimedOut ******");
			showBufferAllotments(buffers);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println();
		System.out.println("---------TEST-6----------");
		System.out.println("6. Try to pin a new buffer again, and check your replacement policy while seeing which currently unpinned buffer is replaced.");
		try {
			System.out.println("Unpinned buffer 3");
			bufferMgr.unpin(buffers[2]);
			System.out.println("Unpinned buffer 2");
			bufferMgr.unpin(buffers[1]);
			showBufferAllotments(buffers);
			System.out.println("Available buffers : " + bufferMgr.available());
			System.out.println("Buffers 2, 3 and 7 are available. Unpinned in reverse order. Attempting to pin now. Thus, according to replacement policy, Buffer 2 should be replaced first, followed by Buffer 3 and Buffer 7.");
			
			bufferMgr.pin(blks[9]);
			showBufferAllotments(buffers);
			bufferMgr.pin(blks[10]);
			showBufferAllotments(buffers);
			bufferMgr.pin(blks[11]);
			showBufferAllotments(buffers);
			System.out.println("Replacement Policy validated.");
		} catch (simpledb.buffer.BufferAbortException e) {
			// Unexpected behavior.
			System.out.println("****** Buffer Abort Exception thrown - TimedOut ******");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}