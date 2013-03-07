package org.apache.pig.test.storm;

import static org.junit.Assert.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.pig.impl.storm.state.WindowBuffer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestWindowStruct {	
	int getTestSum(WindowBuffer<IntWritable> tw) {
		int test_sum = 0;
		for (IntWritable iw: tw.getWindow()) {
			test_sum += iw.get();
		}
		return test_sum;
	}
	
	@Test 
	public void testAddRemove() {
		WindowBuffer<IntWritable> tw = new WindowBuffer<IntWritable>(10);
		
		// Add stuff.
		tw.push(new IntWritable(1));
		tw.push(new IntWritable(2));
		tw.push(new IntWritable(3));
		tw.push(new IntWritable(4));
		tw.push(new IntWritable(2));
		
		// Check the sum.
		assertEquals(12, getTestSum(tw));
		
		// Remove some things.
		// A duplicate value in the middle.
		tw.removeItem(new IntWritable(2));
		assertEquals(10, getTestSum(tw));
		
		// The head.
		tw.removeItem(new IntWritable(1));
		assertEquals(9, getTestSum(tw));
		
		// The tail.
		tw.removeItem(new IntWritable(2));
		assertEquals(7, getTestSum(tw));		
		
		// The rest.
		tw.removeItem(new IntWritable(3));
		assertEquals(4, getTestSum(tw));
		tw.removeItem(new IntWritable(4));
		assertEquals(0, getTestSum(tw));
	}
	
	@Test
    public void testPush() throws Exception {
		WindowBuffer<IntWritable> tw = new WindowBuffer<IntWritable>(10);
		
		int cur_sum = 0;
		for (int i = 0; i < 100; i++) {
			// Update what the sum should be for the contents of tw.
			cur_sum += i;
			if (i >= 10) {
				cur_sum -= i-10;
			}
			
			// Add the new integer.
			tw.push(new IntWritable(i));
			
			// Pull the window and check the sum.
			assertEquals(cur_sum, getTestSum(tw));
		}
	}
}
