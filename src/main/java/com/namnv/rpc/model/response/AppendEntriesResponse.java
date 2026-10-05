package com.namnv.rpc.model.response;

import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serial;
import java.io.Serializable;

@ToString
public class AppendEntriesResponse implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    public final long term;
    public final boolean success;
    public final long matchIndex; // highest index matched on follower (or 0)


    @JsonCreator
    public AppendEntriesResponse(@JsonProperty("term") long term,
            @JsonProperty("success") boolean success,
            @JsonProperty("matchIndex") long matchIndex) {
        this.term = term;
        this.success = success;
        this.matchIndex = matchIndex;
    }
}