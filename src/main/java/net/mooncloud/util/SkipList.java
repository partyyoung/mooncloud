package net.mooncloud.util;

import java.util.ArrayList;
import java.util.Collection;

/**
 * 跳表是一种随机化的数据结构，它的效率和红黑树以及 AVL 树不相上下
 * <p>
 * 跳表具有如下性质：
 * <p>
 * 
 * <pre>
 *      (1) 由很多层结构组成
 *      (2) 每一层都是一个有序的链表
 *      (3) 最底层(Level 1)的链表包含所有元素
 *      (4) 如果一个元素出现在 Level i 的链表中，则它在 Level i 之下的链表也都会出现。
 *      (5) 每个节点包含两个指针，一个指向同一链表中的下一个元素，一个指向下面一层的元素。
 * </pre>
 * 
 * @author yangjd
 *
 * @param <T>
 */
public class SkipList<T extends Comparable<T>>
{

	private int K = 1; // 层数
	private int size = 0; // 元素个数
	private int fullsize = 0; // 节点个数

	public int getFullsize()
	{
		return fullsize;
	}

	public void setFullsize(int fullsize)
	{
		this.fullsize = fullsize;
	}

	public int getSize()
	{
		return size;
	}

	public void setSize(int size)
	{
		this.size = size;
	}

	public SkipList()
	{
		ListNode head = new ListNode();
		listHeads.add(head);
	}

	ArrayList<ListNode> listHeads = new ArrayList<ListNode>(K);

	public class ListNode implements Comparable<ListNode>
	{
		private ListNode next = null, down = null, up = null, downnext = null;
		private T data;

		public ListNode()
		{
		}

		public ListNode(T data)
		{
			this.data = data;
		}

		public ListNode getNext()
		{
			return next;
		}

		public void setNext(ListNode next)
		{
			this.next = next;
		}

		public ListNode getDown()
		{
			return down;
		}

		public void setDown(ListNode down)
		{
			this.down = down;
		}

		public T getData()
		{
			return data;
		}

		public void setData(T data)
		{
			this.data = data;
		}

		@Override
		public String toString()
		{
			return this.data.toString();
		}

		@Override
		public int compareTo(ListNode o)
		{
			return this.data.compareTo(o.data);
		}
	}

	public void print()
	{
		int totalSize = 0;
		for (int k = K - 1; k >= 0; k--)
		{
			ListNode head = listHeads.get(k);
			System.out.print(k + ":");
			while (head.next != null)
			{
				totalSize++;
				System.out.print("\t" + head.next.data);
				head = head.next;
			}
			System.out.println();
		}
		System.out.println(totalSize + "/" + size + "=" + (double) totalSize / size);
	}

	public void print1()
	{
		int totalSize = 0;
		for (int k = K - 1; k >= 0; k--)
		{
			ListNode head = listHeads.get(k);
			System.out.print(k + ":" + head.downnext);
			while (head.next != null)
			{
				totalSize++;
				System.out.print("\t" + head.next.downnext);
				head = head.next;
			}
			System.out.println();
		}
		System.out.println(totalSize + "/" + size + "=" + (double) totalSize / size);
	}

	private SkipList<T> merge1(SkipList<T> list)
	{
		ListNode head = listHeads.get(0).next;
		while (head != null)
		{
			this.add(head);
			head = head.next;
		}
		return this;
	}

	public SkipList<T> merge(final SkipList<T> list)
	{
		return this.merge1(list);
	}

	public void addAll(final Collection<T> datas)
	{
		for (T data : datas)
			add(data);
	}

	public void add(final ListNode newNode)
	{
		if (newNode == null)
		{
			throw new IllegalArgumentException("newNode == null");
		}
		if (newNode.data == null)
		{
			throw new IllegalArgumentException("newNode.data == null");
		}
		int k = getNodeLevel();
		ListNode upNode = null;
		ListNode head = listHeads.get(k);
		while (head != null)
		{
			if (head.next == null)
			{
				head.next = newNode;
				if (head.up != null)
				{
					head.up.downnext = newNode;
				}
				if (upNode != null)
				{
					upNode.down = newNode;
					newNode.up = upNode;
				}
				upNode = newNode;
				head = head.down;
				fullsize++;
			}
			else if (head.next.data.compareTo(newNode.data) >= 0)
			{
				newNode.next = head.next;
				head.next = newNode;
				if (upNode != null)
				{
					upNode.down = newNode;
					newNode.up = upNode;
				}
				if (head.up != null)
				{
					head.up.downnext = newNode;
				}
				if (newNode.up != null)
				{
					newNode.up.downnext = newNode.next;
				}
				upNode = newNode;
				head = head.down;
				fullsize++;
			}
			else if (head.next.data.equals(newNode.data))
			{
				head = head.next;
			}
			else if (head.next.data.compareTo(newNode.data) < 0)
			{
				head = head.next;
			}
		}
		size++;
	}

	public void add(final T data)
	{
		add(new ListNode(data));
	}

	private int getNodeLevel()
	{
		if (size == 0)
		{
			return K - 1;
		}
		int k = 0, maxLevel = K;
		while (Math.random() < 0.5)
		{
			k++;
			if (k == maxLevel)
			{
				ListNode head = new ListNode();
				head.down = listHeads.get(K - 1);
				head.down.up = head;
				listHeads.add(head);
				K++;
				break;
			}
		}
		return k;
	}

	public ListNode getLevel0HeadNode()
	{
		return listHeads.get(0);
	}

	public ListNode searchLevel0(final T data)
	{
		ListNode node = search(data);
		if (node == null)
		{
			return null;
		}
		while (node.down != null)
		{
			node = node.down;
		}
		return node;
	}

	public ListNode search(final T data)
	{
		if (data == null)
		{
			throw new IllegalArgumentException("data == null");
		}
		ListNode head = listHeads.get(K - 1);
		while (head != null)
		{
			if (head.next != null && head.next.data.compareTo(data) == 0)
			{
				return head.next;
			}
			if (head.next == null || head.next.data.compareTo(data) >= 0)
			{
				if (head.downnext == null)
				{
					// System.out.println(data + " < null ↓");
					head = head.down;
				}
				else if (head.downnext.data.compareTo(data) >= 0)
				{
					// System.out.println(data + " < " + head.downnext.data +
					// " ↓");
					head = head.down;
				}
				else
				{
					// System.out.println(head.downnext.data + " < " + data +
					// " ↘");
					head = head.downnext;
				}
			}
			else
			{
				// System.out.println(head.next.data + " < " + data + " →");
				head = head.next;
			}
		}
		return null;
	}

	public void delect(final T data)
	{

	}

	public static void main(String[] args)
	{
		long start = System.currentTimeMillis();
		SkipList<Integer> list = new SkipList<Integer>();
		int c = 100;
		while (c-- > 0)
		{
			int r = (int) (Math.random() * Integer.MAX_VALUE);
			list.add(r);
		}
		int r = (int) (Math.random() * Integer.MAX_VALUE);
		list.add(r);
		long end = System.currentTimeMillis();
		System.out.println("插入时间：" + (end - start));
		list.print();
		list.print1();
		System.out.println(list.getFullsize() + "/" + list.getSize());
		start = System.currentTimeMillis();
		System.out.println("search(" + r + ")=" + list.search(r));
		end = System.currentTimeMillis();
		System.out.println("查找时间：" + (end - start));
	}
}
