package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    /**
     * 利用Redis进行缓存
     * @return
     */
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public List<ShopType> listByCache() {
        String key = RedisConstants.CACHE_SHOPTYPE_KEY;
        //首先从Redis从获取
        List<String> shopTypes = stringRedisTemplate.opsForList().range(key,0,9);
        //判断
        List<ShopType> shopTypesByRedis = new ArrayList<>();
        if (shopTypes.size()!=0) {
            //表示可以查询到
            //进行转换
            for (String shopType : shopTypes) {
                ShopType type = JSONUtil.toBean(shopType, ShopType.class);
                shopTypesByRedis.add(type);
            }
            return shopTypesByRedis;
        }
        //未命中
        List<ShopType> shopTypesByMysql = query().orderByAsc("sort").list();
        //存入redis中
        for (ShopType shopType : shopTypesByMysql) {
            String s = JSONUtil.toJsonStr(shopType);
            stringRedisTemplate.opsForList().rightPushAll(key,s);
        }
        return shopTypesByMysql;
    }
}