package com.namnv.rpc.model.response;

import lombok.ToString;

import java.io.Serial;
import java.io.Serializable;

@ToString
public class AppendEntriesResponse implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    public final long term;
    public final boolean success;
    public final long matchIndex; // highest index matched on follower (or 0)


    public AppendEntriesResponse(long term, boolean success, long matchIndex) {
        this.term = term;
        this.success = success;
        this.matchIndex = matchIndex;
    }
}