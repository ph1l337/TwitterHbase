package com.gpjpe.domain;

public class HashtagCount {
    private String hashtag;
    private int count;

    public HashtagCount(String hashtag, int count) {
        this.hashtag = hashtag;
        this.count = count;
    }

    public String getHashtag() {
        return hashtag;
    }

    public void setHashtag(String hashtag) {
        this.hashtag = hashtag;
    }


    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
