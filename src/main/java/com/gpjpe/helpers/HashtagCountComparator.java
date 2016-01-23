package com.gpjpe.helpers;

import com.gpjpe.domain.HashtagCount;

import java.text.Collator;
import java.util.Comparator;


public class HashtagCountComparator implements Comparator<HashtagCount> {
    @Override
    public int compare(HashtagCount hashtagCount1, HashtagCount hashtagCount2) {
        int hashComparison = Integer.compare(hashtagCount1.getCount(),hashtagCount2.getCount());

        if (hashComparison == 0){
            return (Collator.getInstance().compare(hashtagCount2.getHashtag(), hashtagCount1.getHashtag()));
        }

        return  hashComparison;
    }
}
