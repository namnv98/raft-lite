package com.namnv.rpc.model.response;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class InstallSnapshotResponse implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private long term;
    private boolean success;
}
