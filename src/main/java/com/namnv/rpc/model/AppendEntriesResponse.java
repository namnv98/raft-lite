package com.namnv.rpc.model;

import lombok.ToString;

@ToString
public class AppendEntriesResponse {
    public final long term;
    public final boolean success;
    public final long matchIndex; // highest index matched on follower (or 0)


    public AppendEntriesResponse(long term, boolean success, long matchIndex) {
        this.term = term;
        this.success = success;
        this.matchIndex = matchIndex;
    }
}