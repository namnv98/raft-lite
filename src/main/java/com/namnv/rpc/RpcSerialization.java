package com.namnv.rpc;

import java.io.ObjectInputFilter;

public final class RpcSerialization {

    // chỉ cho deserialize các message của Raft, chặn gadget class từ peer không tin cậy
    public static final ObjectInputFilter FILTER = ObjectInputFilter.Config.createFilter(
            "com.namnv.rpc.model.**;com.namnv.entity.**;java.util.*;java.lang.*;!*");

    private RpcSerialization() {
    }
}
