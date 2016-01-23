package com.gpjpe;

import com.gpjpe.domain.HashtagCount;
import com.gpjpe.helpers.HashtagCountComparator;
import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;


public class HashtagCountComparatorTest extends TestCase {
    private final static Logger LOGGER = LoggerFactory.getLogger(HashtagCountComparatorTest.class.getName());
    Comparator comparator = new HashtagCountComparator();
    HashtagCount a = new HashtagCount("a", 10);
    HashtagCount aa = new HashtagCount("a", 10);
    HashtagCount b = new HashtagCount("b", 10);
    HashtagCount c = new HashtagCount("c", 11);

    public void testComparison() {
        assertEquals(1, comparator.compare(a, b));
        assertEquals(0, comparator.compare(a, aa));
        assertEquals(-1, comparator.compare(b, a));
        assertEquals(-1, comparator.compare(b, c));
        assertEquals(-1, comparator.compare(new HashtagCount("Abc", 10), new HashtagCount("abc", 10)));
        assertEquals(1, comparator.compare(new HashtagCount("abc", 10), new HashtagCount("Abc", 10)));
        assertEquals(1, comparator.compare(new HashtagCount("1abc", 10), new HashtagCount("abc", 10)));
        assertEquals(1, comparator.compare(new HashtagCount("a1bc", 10), new HashtagCount("a2bc", 10)));
        assertEquals(1, comparator.compare(new HashtagCount("1", 10), new HashtagCount("2", 10)));
        assertEquals(1, comparator.compare(new HashtagCount("1", 10), new HashtagCount("2", 8)));
        assertEquals(1, comparator.compare(new HashtagCount("2", 10), new HashtagCount("1", 8)));
        assertEquals(-1, comparator.compare(new HashtagCount("1", 8), new HashtagCount("2", 10)));
    }
}
