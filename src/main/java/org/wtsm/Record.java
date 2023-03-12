package org.wtsm;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class Record {
    private long offset; //偏移量
    private long size; //大小
}
