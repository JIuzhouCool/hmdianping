package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import jodd.util.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result queryById(Long id) {
        //缓存穿透
//        Shop shop = queryWithPassThrough(id);
        //用互斥锁解决缓存击穿
//        Shop shop = queryWithMutex(id);
        //3.使用逻辑过期解决缓存击穿
        Shop shop = queryWithLogicalExpire(id);
        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    /**
     * 基于互斥锁解决缓存击穿
     * @param id
     * @return
     */
    public Shop queryWithMutex(Long id){
        String key = RedisConstants.CACHE_SHOP_KEY+id;
        //首先到redis中查询是否有
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //表示存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //看看是否命中的是空字符串,解决缓存穿透设置的缓存空对象
        if (shopJson!=null){
            return null;
        }
        //不存在，实现缓存重建
        String lockKey = RedisConstants.LOCK_SHOP_KEY+id;
        Shop shop = null;
        try {
            //首先获取互斥锁
            boolean flag = tryLock(lockKey);
            //判断是否获取成功，成功则则重建,失败则休眠，然后过段时间重试
            if (!flag){
                //表示失败
                //先休眠
                Thread.sleep(50);
                //然后重试
                return queryWithMutex(id);
            }
            //获取锁成功，二次检查缓存
            shopJson = stringRedisTemplate.opsForValue().get(key);
            //判断是否存在
            if (StrUtil.isNotBlank(shopJson)) {
                //表示存在，直接返回
                shop = JSONUtil.toBean(shopJson, Shop.class);
                return shop;
            }
            //不存在，则调用数据库
            shop = getById(id);
            Thread.sleep(200);//模拟缓存重建的延迟
            if (shop==null){
                //为了解决缓存穿透问题，在Redis中写入空对象
                stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            //存在，写入Redis
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        }catch (InterruptedException e){
            throw new RuntimeException(e);
        }finally {
            //释放互斥锁
            unLock(lockKey);
        }
        return shop;
    }

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
    //创建一个线程池
    private static final ExecutorService CACHE_REBUID_EXECUTOR = Executors.newFixedThreadPool(10);
    /**
     * 基于逻辑过期时间解决缓存击穿
     * @param id
     * @return
     */
    public Shop queryWithLogicalExpire(Long id){
        String key = RedisConstants.CACHE_SHOP_KEY+id;
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
        Shop shop = JSONUtil.toBean(shopJsonObject, Shop.class);
        LocalDateTime expireTime = shopRedisData.getExpireTime();
        //如果没有过期，直接返回商铺信息
        if(expireTime.isAfter(LocalDateTime.now())){
            //表示没有过期
            return shop;
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
            shop = JSONUtil.toBean(shopJsonObject, Shop.class);
            expireTime = shopRedisData.getExpireTime();
            //如果没有过期，直接返回商铺信息
            if(expireTime.isAfter(LocalDateTime.now())){
                //表示没有过期
                return shop;
            }
            //过期了，则获取一个线程进行重建
            CACHE_REBUID_EXECUTOR.submit(()->{
                try {
                    saveShop2Redis(id,20L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    //释放锁
                    unLock(lockKey);
                }
            });
        }
        //不成功则返回旧的缓存信息
        return shop;
    }
    /**
     * 缓存穿透解决方案
     * @param id
     * @return
     */
    public Shop queryWithPassThrough(Long id){
        String key = RedisConstants.CACHE_SHOP_KEY+id;
        //首先到redis中查询是否有
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            //表示存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        //不存在，看看是否命中的是空字符串""
        if (shopJson!=null){
            return null;
        }
        //不存在，直接到数据库中查询
        Shop shop = getById(id);
        if (shop==null){
            //为了解决缓存穿透问题，在Redis中写入空对象
            stringRedisTemplate.opsForValue().set(key,"",RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //存在，写入Redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }

    /**
     * 更新
     * @param shop
     * @return
     */
    @Override
    @Transactional
    public Result update(Shop shop) {
        //首先判断能不能更新
        if(shop.getId()==null){
            return Result.fail("店铺id不能为空");
        }
        //首先更新数据库
        updateById(shop);
        //然后删除缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY+shop.getId());
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //判断是否需要根据x,y查询
        if (x==null||y==null){
            //不需要，直接查询数据库
            // 根据类型分页查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }
        //计算分页参数
        int from = (current-1)*SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current*SystemConstants.DEFAULT_PAGE_SIZE;
        //查询，按照距离排序，分页，结果:shopId,distance
        String key = RedisConstants.SHOP_GEO_KEY+typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> geoResults = stringRedisTemplate
                .opsForGeo()
                .search(key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end));//查询到0-end，然后截取
        if (geoResults==null){
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> content = geoResults.getContent();
        //截取从from到end
        List<Long> ids = new ArrayList<>(content.size());
        Map<String,Distance> map = new HashMap<>(content.size());
        if(content.size()<from){
            return Result.ok(Collections.emptyList());
        }
        content.stream().skip(from).forEach(result->{
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            //获取距离
            Distance distance = result.getDistance();
            map.put(shopIdStr,distance);
        });
        String idStr = StringUtil.join(ids, ",");
        //根据id查询shop
        List<Shop> shopList = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shopList) {
            shop.setDistance(map.get(shop.getId().toString()).getValue());
        }
        //返回
        return Result.ok(shopList);
    }

    public void saveShop2Redis(Long id,Long expireSeconds) throws InterruptedException {
        Shop shop = getById(id);
        //封装成RedisData
        RedisData<Shop> shopRedisData = new RedisData<>();
        shopRedisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        shopRedisData.setData(shop);
        Thread.sleep(200);//模拟缓存重建的过程
        //写入Redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(shopRedisData));
    }
}
