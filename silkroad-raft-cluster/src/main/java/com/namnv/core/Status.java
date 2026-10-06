package com.namnv.core;

import lombok.Getter;

@Getter
public class Status {
    private final boolean ok;
    private final String msg;

    private Status(boolean ok, String msg) {
        this.ok = ok;
        this.msg = msg;
    }

    public static Status OK() {
        return new Status(true, "OK");
    }

    public static Status ERROR(String msg) {
        return new Status(false, msg);
    }
}
