package org.apache.pig.impl.storm.state;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.pig.backend.hadoop.HDataType;

public class WindowBuffer<T extends Writable> implements Writable {

	private int maxSize;
	
	// Maps object value to the head of the value's linked list.
	Map<Writable, Element> valMap;
	ArrayList<Element> window;
	int[] free_list;
	int alloc_size = 0;
	int head;
	int tail;
	int cur_count;
	
	// Going to create a doubly linked list to manage this.
	class Element {
		// Can traverse in the value direction.
		Element nextByValue;
		
		// Or for purposes of the window.
		Element prevByWindow;
		Element nextByWindow;
		
		int arr_pos;
		
		// The following are only valid for the head of the value list.		
		// The value itself.
		Writable val;
		// The number of times this value appears.
		int count;
		// And the last element with this value.
		Element tailByValue;
		
		void reset() {
			nextByValue = prevByWindow = nextByWindow = tailByValue = null;
			val = null;
			count = 0;
		}
		
		void configure(Writable val, int count, Element tailByValue) {
			this.val = val;
			this.count = count;
			this.tailByValue = tailByValue;
		}
		
		public String toString() {
			return "ElementBuffer(" + val + ", " + count + ", " + tailByValue + ", " + (prevByWindow != null) + ", " + (nextByWindow != null) + ")";
		}
	}
	
	public WindowBuffer() {
		
	}
	
	public WindowBuffer(int maxSize) {
		init(maxSize);
	}
	
	void init(int maxSize) {
		this.maxSize = maxSize;
		window = new ArrayList<Element>(maxSize);
		valMap = new HashMap<Writable, Element>(maxSize);
		free_list = new int[maxSize];
		head = tail = -1;
		cur_count = 0;
	}

	void addToWindowTail(Element e) {
		if (tail == -1) {
			// this is the new tail!
			head = tail = e.arr_pos;
		} else {
			// Update the tail.
			Element tail_e = window.get(tail);
			tail_e.nextByWindow = e;
			e.prevByWindow = tail_e;
			tail = e.arr_pos;
		}
	}
	
	void addToValueTail(Element head, Element e) {
		// Increment the count.
		head.count += 1;
		
		// Update the tail.
		head.tailByValue.nextByValue = e;
		head.tailByValue = e;
	}
	
	Element getFreeElement() {
		Element ret = null;
		
		// First see if we have something in the free list.
		if (alloc_size - cur_count > 0) {
			ret = window.get(free_list[alloc_size - cur_count - 1]);
		} else {
			// We're out of luck, allocate a new element.
			ret = new Element();
			ret.arr_pos = window.size();
			window.add(ret);
			alloc_size++;
		}
		
		cur_count++;

		return ret;
	}
	
	void freeElement(Element e) {
		// Put the element on the free list.
		free_list[alloc_size-cur_count] = e.arr_pos;
		// Wipe out the element data.
		e.reset();
		// Decrement the current counter.
		cur_count--;
	}
	
	public void push(Writable o) {
		// If we're over size, remove the head.
		if (cur_count == maxSize) {
			T head_o = (T) window.get(head).val;
			removeItem(head_o);
		}
				
		// See if we have this value in the data structure.
		Element el = valMap.get(o);
		if (el == null) {
			// We do not, create a new element and populate things appropriately.
			el = getFreeElement();
			el.val = o;
			el.count = 1;
			el.tailByValue = el;
			
			// Add it to the map.
			valMap.put(o, el);

			// Link the element to the tail.
			addToWindowTail(el);
		} else {
			// We need an element for the next position.
			Element next_el = getFreeElement();
			// Link it to the last in this value chain.
			addToValueTail(el, next_el);	
			// Link to the window tail
			addToWindowTail(next_el);
		}
	}
	
	public void removeItem(T o) {
		// FIXME: Handle removeItem for elements that have negative values before positive.
		
		// Get the head of the value list.
		Element el = valMap.get(o);
		
		// Decrement the counter.
		el.count--;
		
		// Update the nextByValue (if any)
		if (el.count > 0) {
			Element el_n = el.nextByValue;
			el_n.configure(el.val, el.count, el.tailByValue);
			valMap.put(o, el_n);
		} else {
			// Clean up the map.
			valMap.remove(o);
		}
		
		// Delink from the window
		Element next = el.nextByWindow;
		Element prev = el.prevByWindow;
		if (next != null && prev != null) {
			// Easy, we're in the middle.
			prev.nextByWindow = next;
			next.prevByWindow = prev;
		} else if (next == null && prev != null) {
			// We are the tail.
			prev.nextByWindow = null;
			tail = prev.arr_pos;
		} else if (next != null && prev == null) {
			// We are the head.
			next.prevByWindow = null;
			head = next.arr_pos;
		} else {
			// The window is now empty.
			head = tail = -1;
		}
		
		// "free" the element.
		freeElement(el);
	}
	
	public List<T> getWindow() {
		ArrayList<T> ret = new ArrayList<T>(maxSize);
		
		if (cur_count == 0) {
			return ret;
		}
		
		Element cur = window.get(head);
		while(cur != null) {
			if (cur.val != null) {
				for (int i = 0; i < cur.count; i++) {
					ret.add((T) cur.val);
				}
			}
			
			cur = cur.nextByWindow;
		}
		
		return ret;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		// Write the max size.
		out.writeInt(maxSize);
		
		// Write if the window is empty.
		out.writeBoolean(cur_count > 0);

		if (cur_count > 0) {
			// The following only works if we have something in the window.
			List<T> win = getWindow();			
			
			// Write the class name.
			Class<? extends Writable> klass = win.get(0).getClass();
			out.writeUTF(klass.getName());
			
			ArrayWritable aw = new ArrayWritable(klass, win.toArray(new Writable[0]));
			aw.write(out);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// Read the max.
		int max = in.readInt();
		// Init
		init(max);
		
		// Read to see if we have data.
		boolean has_data = in.readBoolean();
		if (has_data) {
			// Pull the class name
			String kn = in.readUTF();
			// Instantiate it
			Class<? extends Writable> klass;
			try {
				klass = (Class<? extends Writable>) Class.forName(kn);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
			
			// Pull the array.
			ArrayWritable aw = new ArrayWritable(klass);
			aw.readFields(in);
			
			for (Writable o : aw.get()) {
				push(o);
			}	
		}
	}
	
	public String toString() {
		return "WindowBuffer(maxSize: " + maxSize + ", cur_count: " + cur_count + ")";
	}
	
	public int size() {
		return cur_count;
	}
}
