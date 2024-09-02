package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Component
@Slf4j
public class CacheClientUtil {
    private StringRedisTemplate stringRedisTemplate;

    public CacheClientUtil(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }
    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        //添加逻辑过期时间进行封装
        RedisData<Object> redisData = new RedisData<>();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 缓存穿透解决方案
     * @param id
     * @return
     */
    public <R,ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback, Long time, TimeUnit unit){
        String key = keyPrefix+id;
        //首先到redis中查询是否有
        String json = stringRedisTemplate.opsForValue().get(key);
        //判断是否存在
        if (StrUtil.isNotBlank(json)) {
            //表示存在，直接返回
             return JSONUtil.toBean(json, type);
        }
        //不存在，看看是否命中的是空字符串""
        if (json!=null){
            return null;
        }
        //不存在，直接到数据库中查询
        R r = dbFallback.apply(id);
        if (r == null){
            //为了解决缓存穿透问题，在Redis中写入空对象
            stringRedisTemplate.opsForValue().set(key,"",time,unit);
            return null;
        }
        //存在，写入Redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(r),time,unit);
        return r;
    }

    //创建线程池
    private static final ExecutorService CACHE_REBUID_EXECUTOR = Executors.newFixedThreadPool(10);
    /**
     * 获取锁
     * @param key
     * @return
     */
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", RedisConstants.LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放锁
     * @param key
     */
    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }

    /**
     * 基于逻辑过期时间解决缓存击穿
     * @param id
     * @return
     */
    public <R,ID> R queryWithLogicalExpire(String keyPrefix,ID id,Class<R> type,Function<ID,R> dbFallback,Long time,TimeUnit unit){
        String key = keyPrefix+id;
        //首先到redis中查询是否有
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //判断是否存在
        if (StrUtil.isBlank(shopJson)) {
            //表示不存在，返回空
            return null;
        }
        //命中，判断是否逻辑过期
        RedisData shopRedisData = JSONUtil.toBean(shopJson, RedisData.class);
        JSONObject shopJsonObject = (JSONObject) shopRedisData.getData();
        R r = JSONUtil.toBean(shopJsonObject,type);
        LocalDateTime expireTime = shopRedisData.getExpireTime();
        //如果没有过期，直接返回商铺信息
        if(expireTime.isAfter(LocalDateTime.now())){
            //表示没有过期
            return r;
        }
        //如果过期，获取互斥锁，开启独立线程进行缓存重建
        String lockKey = RedisConstants.LOCK_SHOP_KEY+id;
        boolean isLock = tryLock(lockKey);
        //判断互斥锁是否可以获取成功
        if (isLock){
            //成功进行缓存的重建
            //开启独立的线程，但是在开启独立线程之前，要进行Redis数据的双重检验
            shopJson = stringRedisTemplate.opsForValue().get(key);
            //命中，判断是否逻辑过期
            shopRedisData = JSONUtil.toBean(shopJson, RedisData.class);
            shopJsonObject = (JSONObject) shopRedisData.getData();
            r = JSONUtil.toBean(shopJsonObject, type);
            expireTime = shopRedisData.getExpireTime();
            //如果没有过期，直接返回商铺信息
            if(expireTime.isAfter(LocalDateTime.now())){
                //表示没有过期
                return r;
            }
            //过期了，则获取一个线程进行重建
            CACHE_REBUID_EXECUTOR.submit(()-> {
                R r1 = null;
                try {
                    r1 = dbFallback.apply(id);
                    this.setWithLogicalExpire(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unLock(lockKey);
                    return r1;
                }
            });
        }
        //不成功则返回旧的缓存信息
        return r;
    }
}
