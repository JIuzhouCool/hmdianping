package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.BooleanUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock {
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    private String name;
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    public SimpleRedisLock(StringRedisTemplate stringRedisTemplate, String name) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.name = name;
    }

    private static final String KEY_PREFIX = "lock:";
    private static final String ID_PREFIX = UUID.randomUUID().toString(true) + "-";

    @Override
    public boolean tryLock(long timeOutSec) {
        //获取当前进程的id,当作value
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(KEY_PREFIX + name, threadId, timeOutSec, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    //    @Override
//    public void unLock() {
//        //首先获取进程id
//        String threadId = ID_PREFIX+Thread.currentThread().getId();
//        //获取锁的value
//        String redisId = stringRedisTemplate.opsForValue().get(KEY_PREFIX + name);
//        if (threadId.equals(redisId)){
//            stringRedisTemplate.delete(KEY_PREFIX + name);
//        }
//    }
    //使用lua脚本一致性问题释放锁
    @Override
    public void unLock() {
        //调用lua脚本

        stringRedisTemplate.execute(
                UNLOCK_SCRIPT,
                Collections.singletonList(KEY_PREFIX + name),
                ID_PREFIX+Thread.currentThread().getId()
                );
    }
}
