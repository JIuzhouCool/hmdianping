package com.hmdp.utils;

public interface ILock {
    /**
     * 尝试获取分布式锁
     * @param timeOutSec
     * @return
     */
    boolean tryLock(long timeOutSec);

    /**
     * 释放锁
     */
    void unLock();
}
